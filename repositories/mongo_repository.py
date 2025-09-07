"""
Repositório para operações com MongoDB
Implementa o padrão Repository (DDD)
"""
import logging
from datetime import datetime
from typing import List, Dict, Any

from pymongo import MongoClient
from interfaces.abstractions import IConversationRepository

logger = logging.getLogger(__name__)


class MongoConversationRepository(IConversationRepository):
    """Repositório MongoDB para conversas"""

    def __init__(self, mongo_uri: str, database_name: str, collection_name: str):
        self._mongo_uri = mongo_uri
        self._database_name = database_name
        self._collection_name = collection_name
        self._client = None
        self._collection = None

    def _connect(self) -> None:
        """Conecta ao MongoDB"""
        if not self._client:
            self._client = MongoClient(self._mongo_uri)
            db = self._client[self._database_name]
            self._collection = db[self._collection_name]
            logger.info("✅ Conectado ao MongoDB")

    def find_by_date_range(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Busca conversas por intervalo de datas"""
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

    def close_connection(self) -> None:
        """Fecha conexão com MongoDB"""
        if self._client:
            self._client.close()
            self._client = None
            self._collection = None
            logger.info("🔒 Conexão MongoDB fechada")
