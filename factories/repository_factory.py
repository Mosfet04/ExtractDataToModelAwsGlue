"""
Factory para criação de repositórios
Implementa o padrão Factory e Dependency Injection
"""
import logging
import os
from typing import Optional

from interfaces.abstractions import IConversationRepository
from repositories.mongo_repository import MongoConversationRepository
from repositories.simulated_repository import SimulatedConversationRepository

logger = logging.getLogger(__name__)


class RepositoryFactory:
    """Factory para criação de repositórios de conversas"""

    @staticmethod
    def create_conversation_repository(
        mongo_uri: Optional[str] = None,
        database_name: Optional[str] = None,
        collection_name: Optional[str] = None
    ) -> IConversationRepository:
        """
        Cria repositório de conversas baseado na disponibilidade do MongoDB

        Args:
            mongo_uri: URI do MongoDB
            database_name: Nome do banco
            collection_name: Nome da coleção

        Returns:
            Repositório configurado
        """
        # Usar valores padrão do ambiente se não fornecidos
        mongo_uri = mongo_uri or os.environ.get('MONGO_URI')
        database_name = database_name or os.environ.get('DATABASE_NAME', 'conversations')
        collection_name = collection_name or os.environ.get('COLLECTION_NAME', 'chat_data')

        # Verificar disponibilidade do MongoDB
        mongodb_available = RepositoryFactory._check_mongodb_availability()

        if mongodb_available and mongo_uri:
            logger.info("✅ Usando repositório MongoDB")
            return MongoConversationRepository(mongo_uri, database_name, collection_name)
        else:
            logger.warning("⚠️ MongoDB não disponível, usando repositório simulado")
            return SimulatedConversationRepository()

    @staticmethod
    def _check_mongodb_availability() -> bool:
        """Verifica se o MongoDB está disponível"""
        try:
            import pymongo
            return True
        except ImportError:
            return False
