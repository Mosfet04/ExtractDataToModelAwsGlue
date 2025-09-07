"""
Uploader S3 refatorado
Implementa Interface Segregation Principle
"""
import logging
from datetime import datetime, timezone
from typing import Dict, Any

import boto3
from botocore.exceptions import ClientError

from interfaces.abstractions import IFileUploader
from config.constants import CSVConfig

logger = logging.getLogger(__name__)


class S3FileUploader(IFileUploader):
    """Uploader especializado para S3"""

    def __init__(self):
        self._s3_client = boto3.client('s3')

    def upload_file(self, content: str, bucket: str, key: str) -> Dict[str, Any]:
        """
        Faz upload de arquivo para S3

        Args:
            content: Conteúdo do arquivo
            bucket: Nome do bucket S3
            key: Chave do arquivo no S3

        Returns:
            Resultado do upload
        """
        try:
            # Preparar metadados
            metadata = self._prepare_metadata()

            # Upload
            self._s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=content.encode(CSVConfig.DEFAULT_ENCODING),
                ContentType=CSVConfig.MIME_TYPE,
                Metadata=metadata
            )

            # Preparar resultado
            result = self._prepare_success_result(content, bucket, key)

            logger.info(f"✅ Arquivo enviado para S3: s3://{bucket}/{key}")
            logger.info(f"📊 Tamanho: {result['file_size_bytes']} bytes")

            return result

        except ClientError as e:
            logger.error(f"❌ Erro do S3: {e}")
            return self._prepare_error_result(str(e))
        except Exception as e:
            logger.error(f"❌ Erro no upload: {e}")
            return self._prepare_error_result(str(e))

    def _prepare_metadata(self) -> Dict[str, str]:
        """Prepara metadados do arquivo"""
        return {
            'generated_by': 'conversation_extractor_glue',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'version': '2.0'
        }

    def _prepare_success_result(self, content: str, bucket: str, key: str) -> Dict[str, Any]:
        """Prepara resultado de sucesso"""
        file_size = len(content.encode(CSVConfig.DEFAULT_ENCODING))

        return {
            "success": True,
            "s3_url": f"s3://{bucket}/{key}",
            "bucket": bucket,
            "key": key,
            "file_size_bytes": file_size,
            "upload_timestamp": datetime.now(timezone.utc).isoformat()
        }

    def _prepare_error_result(self, error_message: str) -> Dict[str, Any]:
        """Prepara resultado de erro"""
        return {
            "success": False,
            "error": error_message,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
