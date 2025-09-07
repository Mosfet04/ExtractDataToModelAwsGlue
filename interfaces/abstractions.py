"""
Interfaces para abstrair dependências externas
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any


class IConversationRepository(ABC):
    """Interface para repositório de conversas"""

    @abstractmethod
    def find_by_date_range(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Busca conversas por intervalo de datas"""
        pass

    @abstractmethod
    def close_connection(self) -> None:
        """Fecha conexão com o repositório"""
        pass


class ICSVGenerator(ABC):
    """Interface para geração de CSV"""

    @abstractmethod
    def generate_csv(self, conversations: List[Dict[str, Any]]) -> str:
        """Gera CSV a partir das conversas"""
        pass


class IFileUploader(ABC):
    """Interface para upload de arquivos"""

    @abstractmethod
    def upload_file(self, content: str, bucket: str, key: str) -> Dict[str, Any]:
        """Faz upload do arquivo"""
        pass


class IMetricsCalculator(ABC):
    """Interface para cálculo de métricas"""

    @abstractmethod
    def calculate(self, run_metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calcula métricas específicas"""
        pass


class IConversationExtractor(ABC):
    """Interface para extração de conversas"""

    @abstractmethod
    def extract(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Extrai conversas por intervalo de datas"""
        pass
