"""
AWS Glue Job para extração de dados do MongoDB, processamento para ML e upload para S3
Adaptado da função Lambda original
"""
import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
from typing import Dict, Any

# Adicionar o caminho para os módulos locais
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.validators import EventValidator
from factories.repository_factory import RepositoryFactory
from services.conversation_extraction_service import ConversationExtractionService
from services.csv_generator_service import TrainingCSVGenerator
from services.s3_upload_service import S3FileUploader
from config.constants import S3Config

# Configurar logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class ConversationExtractionOrchestrator:
    """
    Orquestrador principal que coordena todo o processo
    Implementa o padrão Facade e Single Responsibility
    """

    def __init__(self):
        self._repository = None
        self._extraction_service = None
        self._csv_generator = TrainingCSVGenerator()
        self._s3_uploader = S3FileUploader()

    def process_extraction_request(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processa solicitação de extração de conversas

        Args:
            event: Evento com start_date e end_date

        Returns:
            Resultado do processamento
        """
        try:
            # 1. Validar evento
            self._validate_event(event)

            # 2. Configurar dependências
            self._setup_dependencies()

            # 3. Extrair dados
            start_date = event.get('start_date', (datetime.now(timezone.utc) - relativedelta(months=1)).date().strftime('%Y-%m-%d'))
            end_date = event.get('end_date', (datetime.now(timezone.utc) - timedelta(days=1)).date().strftime('%Y-%m-%d'))

            logger.info(f"🔧 Iniciando extração para {start_date} - {end_date}")

            conversations = self._extraction_service.extract(start_date, end_date)

            # 4. Gerar CSV
            csv_content = self._csv_generator.generate_csv(conversations)

            # 5. Upload para S3
            s3_key = self._generate_s3_key(start_date, end_date)
            bucket = os.environ.get('S3_BUCKET', S3Config.DEFAULT_BUCKET)

            upload_result = self._s3_uploader.upload_file(csv_content, bucket, s3_key)

            # 6. Preparar resultado
            result = self._prepare_success_result(
                start_date, end_date, conversations, csv_content, s3_key, upload_result
            )

            logger.info("✅ Processo concluído com sucesso")
            return result

        except Exception as e:
            logger.error(f"❌ Erro no processo: {e}")
            raise

    def _validate_event(self, event: Dict[str, Any]) -> None:
        """Valida o evento de entrada"""
        error = EventValidator.validate_extraction_event(event)
        if error:
            raise ValueError(error)

    def _setup_dependencies(self) -> None:
        """Configura as dependências necessárias"""
        if not self._repository:
            self._repository = RepositoryFactory.create_conversation_repository()

        if not self._extraction_service:
            self._extraction_service = ConversationExtractionService(self._repository)

    def _generate_s3_key(self, start_date: str, end_date: str) -> str:
        """Gera chave S3 para o arquivo"""
        return f"{S3Config.FOLDER_PREFIX}/{start_date}_to_{end_date}/conversations_{start_date}_to_{end_date}.csv"

    def _prepare_success_result(
        self,
        start_date: str,
        end_date: str,
        conversations: list,
        csv_content: str,
        s3_key: str,
        upload_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Prepara resultado de sucesso"""
        return {
            "message": f"Conversas entre {start_date} e {end_date} extraídas com sucesso",
            "extraction_summary": {
                "start_date": start_date,
                "end_date": end_date,
                "date_range_days": self._calculate_date_range_days(start_date, end_date),
                "total_conversations": len(conversations)
            },
            "csv_info": {
                "size_chars": len(csv_content),
                "size_bytes": len(csv_content.encode('utf-8')),
                "s3_key": s3_key
            },
            "s3_upload": upload_result,
            "processing_timestamp": datetime.now(timezone.utc).isoformat()
        }

    def _calculate_date_range_days(self, start_date: str, end_date: str) -> int:
        """Calcula número de dias no intervalo"""
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        return (end - start).days + 1


def main():
    """
    Função principal do Glue Job
    """
    try:
        logger.info("🚀 Iniciando Glue Job para extração de conversas")

        # Parâmetros do job (podem ser passados via argumentos ou variáveis de ambiente)
        start_date = os.environ.get('START_DATE')
        end_date = os.environ.get('END_DATE')

        if not start_date or not end_date:
            # Valores padrão
            start_date = (datetime.now(timezone.utc) - relativedelta(months=1)).date().strftime('%Y-%m-%d')
            end_date = (datetime.now(timezone.utc) - timedelta(days=1)).date().strftime('%Y-%m-%d')

        event = {
            "start_date": start_date,
            "end_date": end_date
        }

        logger.info(f"📦 Parâmetros: {json.dumps(event, default=str)}")

        # Criar orquestrador e processar
        orchestrator = ConversationExtractionOrchestrator()
        result = orchestrator.process_extraction_request(event)

        logger.info("✅ Glue Job concluído com sucesso")

        # Salvar resultado em arquivo ou log
        print(json.dumps({
            'success': True,
            'process': 'extract_conversations_date_range_for_training',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'result': result
        }, indent=2, default=str))

    except ValueError as ve:
        logger.error(f"❌ Erro de validação: {ve}")
        print(json.dumps({
            'success': False,
            'error': str(ve),
            'example': {
                'start_date': '2024-12-20',
                'end_date': '2024-12-25'
            },
            'timestamp': datetime.now(timezone.utc).isoformat()
        }, indent=2, default=str))
        sys.exit(1)

    except Exception as e:
        logger.error(f"❌ Erro crítico: {e}", exc_info=True)
        print(json.dumps({
            'success': False,
            'process': 'extract_conversations_date_range_for_training',
            'error': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }, indent=2, default=str))
        sys.exit(1)


if __name__ == "__main__":
    main()
