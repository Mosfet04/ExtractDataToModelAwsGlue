"""
Repositório para operações com MongoDB
Implementa o padrão Repository (DDD)
Suporta tanto pymongo quanto Spark DataFrame
"""
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from pymongo import MongoClient
from interfaces.abstractions import IConversationRepository

# Imports opcionais para Spark
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None

logger = logging.getLogger(__name__)


class MongoConversationRepository(IConversationRepository):
    """Repositório MongoDB para conversas com suporte a Spark"""

    def __init__(self, mongo_uri: str, database_name: str, collection_name: str, spark_session: Optional[SparkSession] = None):
        self._mongo_uri = mongo_uri
        self._database_name = database_name
        self._collection_name = collection_name
        self._client = None
        self._collection = None
        self._spark = spark_session

    def _connect(self) -> None:
        """Conecta ao MongoDB"""
        if not self._client:
            self._client = MongoClient(self._mongo_uri)
            db = self._client[self._database_name]
            self._collection = db[self._collection_name]
            logger.info("✅ Conectado ao MongoDB")

    def find_by_date_range(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Busca conversas por intervalo de datas usando pymongo"""
        self._connect()

        # Converter datas para timestamp Unix
        start_timestamp = int(datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S").timestamp())
        end_timestamp = int(datetime.strptime(f"{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S").timestamp())

        query = {
            "created_at": {
                "$gte": start_timestamp,
                "$lte": end_timestamp
            }
        }

        logger.info(f"🔍 Executando query: {query}")

        try:
            documents = list(self._collection.find(query))
            logger.info(f"📊 Documentos encontrados: {len(documents)}")
            return documents
        except Exception as e:
            logger.error(f"❌ Erro na consulta MongoDB: {e}")
            return []

    def find_by_date_range_spark(self, start_date: str, end_date: str) -> Optional[DataFrame]:
        """Busca conversas por intervalo de datas usando Spark DataFrame"""
        if not SPARK_AVAILABLE or not self._spark:
            logger.warning("⚠️ Spark não disponível ou não configurado")
            return None

        # Converter datas para timestamp Unix
        start_timestamp = int(datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S").timestamp())
        end_timestamp = int(datetime.strptime(f"{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S").timestamp())

        try:
            # Ler dados do MongoDB usando Spark
            df = (self._spark.read.format("mongo")
                  .option("database", self._database_name)
                  .option("collection", self._collection_name)
                  .load()
                  .filter((col("created_at") >= start_timestamp) & (col("created_at") <= end_timestamp)))

            logger.info(f"📊 Dados lidos via Spark: {df.count()} registros")
            return df
        except Exception as e:
            logger.error(f"❌ Erro na consulta MongoDB via Spark: {e}")
            return None

    def close_connection(self) -> None:
        """Fecha conexão com MongoDB"""
        if self._client:
            self._client.close()
            self._client = None
            self._collection = None
            logger.info("🔒 Conexão MongoDB fechada")
