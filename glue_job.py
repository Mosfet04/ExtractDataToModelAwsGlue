"""
AWS Glue Job Spark para extração de dados do MongoDB, processamento para ML e upload para S3
Usa PySpark e GlueContext para processamento distribuído
"""
import sys
import os
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Adicionar o caminho para os módulos locais
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.validators import EventValidator
from services.conversation_extraction_service import ConversationExtractionService
from services.s3_upload_service import S3FileUploader
from config.constants import S3Config

# Configurar logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def create_spark_session():
    """Cria SparkSession com configurações para MongoDB"""
    return (SparkSession.builder
            .appName("ConversationExtractionSpark")
            .config("spark.mongodb.input.uri", os.environ.get('MONGO_URI'))
            .config("spark.mongodb.output.uri", os.environ.get('MONGO_URI'))
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
            .getOrCreate())

def read_mongo_data(spark, database, collection, start_date, end_date):
    """Lê dados do MongoDB usando Spark DataFrame"""
    # Converter datas para timestamp
    start_timestamp = int(datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S").timestamp())
    end_timestamp = int(datetime.strptime(f"{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S").timestamp())

    # Ler dados do MongoDB
    df = (spark.read.format("mongo")
          .option("database", database)
          .option("collection", collection)
          .load()
          .filter((col("created_at") >= start_timestamp) & (col("created_at") <= end_timestamp)))

    logger.info(f"📊 Dados lidos do MongoDB: {df.count()} registros")
    return df

def process_conversations_spark(df):
    """Processa conversas usando Spark DataFrame operations"""
    # Aqui podemos adicionar transformações Spark
    # Por enquanto, apenas selecionar campos básicos
    processed_df = df.select(
        col("_id").alias("session_id"),
        col("agent_name"),
        col("model_provider"),
        col("model_id"),
        col("created_at"),
        col("runs")  # Manter runs para processamento posterior
    )

    # Adicionar processamento de métricas usando UDFs se necessário
    # processed_df = processed_df.withColumn("total_runs", size(col("runs")))

    return processed_df

def generate_csv_spark(df, output_path):
    """Gera CSV usando Spark DataFrame"""
    # Escrever como CSV particionado
    (df.write
     .mode("overwrite")
     .option("header", "true")
     .csv(output_path))

    logger.info(f"📄 CSV gerado em: {output_path}")

def upload_to_s3(local_path, bucket, key):
    """Upload do CSV para S3"""
    uploader = S3FileUploader()

    # Como estamos usando Glue, podemos usar o DynamicFrame para upload
    # Por simplicidade, usar o uploader existente
    # Nota: Em produção, seria melhor usar Glue's S3 sink

    # Ler o CSV gerado e fazer upload
    with open(f"{local_path}/part-00000-*.csv", 'r') as f:
        content = f.read()

    result = uploader.upload_file(content, bucket, key)
    logger.info(f"📤 Upload para S3 concluído: {result}")
    return result

def main():
    """Função principal do Glue Job Spark"""
    try:
        logger.info("🚀 Iniciando Glue Job Spark para extração de conversas")

        # Obter argumentos do Glue Job
        args = getResolvedOptions(sys.argv, ['JOB_NAME', 'START_DATE', 'END_DATE', 'MONGO_DATABASE', 'MONGO_COLLECTION', 'S3_BUCKET'])

        start_date = args.get('START_DATE')
        end_date = args.get('END_DATE')
        mongo_database = args.get('MONGO_DATABASE', 'conversations')
        mongo_collection = args.get('MONGO_COLLECTION', 'sessions')
        s3_bucket = args.get('S3_BUCKET', S3Config.DEFAULT_BUCKET)

        if not start_date or not end_date:
            # Valores padrão
            start_date = (datetime.now(timezone.utc) - relativedelta(months=1)).date().strftime('%Y-%m-%d')
            end_date = (datetime.now(timezone.utc) - timedelta(days=1)).date().strftime('%Y-%m-%d')

        logger.info(f"📦 Parâmetros: start_date={start_date}, end_date={end_date}")

        # Criar Spark Session
        spark = create_spark_session()
        sc = SparkContext.getOrCreate()

        # Ler dados do MongoDB
        df = read_mongo_data(spark, mongo_database, mongo_collection, start_date, end_date)

        # Processar dados
        processed_df = process_conversations_spark(df)

        # Gerar CSV
        temp_output_path = "/tmp/conversations_csv"
        generate_csv_spark(processed_df, temp_output_path)

        # Upload para S3
        s3_key = f"{S3Config.FOLDER_PREFIX}/{start_date}_to_{end_date}/conversations_{start_date}_to_{end_date}.csv"
        upload_result = upload_to_s3(temp_output_path, s3_bucket, s3_key)

        # Resultado
        result = {
            "message": f"Conversas entre {start_date} e {end_date} extraídas com sucesso",
            "extraction_summary": {
                "start_date": start_date,
                "end_date": end_date,
                "total_conversations": processed_df.count()
            },
            "s3_upload": upload_result,
            "processing_timestamp": datetime.now(timezone.utc).isoformat()
        }

        logger.info("✅ Glue Job Spark concluído com sucesso")
        print(result)

    except Exception as e:
        logger.error(f"❌ Erro no Glue Job Spark: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
