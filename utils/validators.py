"""
Utilitários para validação de dados
"""
import re
from datetime import datetime
from typing import Optional, Dict, Any


class DateValidator:
    """Validador de datas"""

    @staticmethod
    def validate_date_format(date_str: str) -> bool:
        """Valida se a data está no formato YYYY-MM-DD"""
        if not date_str:
            return False

        pattern = r'^\d{4}-\d{2}-\d{2}$'
        if not re.match(pattern, date_str):
            return False

        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False

    @staticmethod
    def validate_date_range(start_date: str, end_date: str) -> Optional[str]:
        """
        Valida intervalo de datas

        Returns:
            None se válido, string com erro se inválido
        """
        if not DateValidator.validate_date_format(start_date):
            return f"Data inicial inválida: {start_date}"

        if not DateValidator.validate_date_format(end_date):
            return f"Data final inválida: {end_date}"

        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')

        if start_dt > end_dt:
            return "Data inicial deve ser anterior à data final"

        return None


class EventValidator:
    """Validador de eventos"""

    @staticmethod
    def validate_extraction_event(event: Dict[str, Any]) -> Optional[str]:
        """
        Valida evento de extração

        Returns:
            None se válido, string com erro se inválido
        """
        start_date = event.get('start_date')
        end_date = event.get('end_date')

        if not start_date:
            return "Campo 'start_date' é obrigatório"

        if not end_date:
            return "Campo 'end_date' é obrigatório"

        return DateValidator.validate_date_range(start_date, end_date)
