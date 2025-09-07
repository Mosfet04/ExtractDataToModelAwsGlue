"""
Gerador de CSV refatorado
Segue o Single Responsibility Principle

⚠️ DEPRECIADO: Este serviço está sendo descontinuado.
A geração de CSV agora é feita de forma nativa e distribuída pelo Spark no glue_job_spark.py
usando DataFrame.write.csv() para melhor performance e escalabilidade.
"""
import csv
import io
import logging
from typing import List, Dict, Any

from interfaces.abstractions import ICSVGenerator
from config.constants import CSVConfig

logger = logging.getLogger(__name__)


class TrainingCSVGenerator(ICSVGenerator):
    """Gerador de CSV específico para dados de treinamento"""

    def __init__(self):
        logger.warning("⚠️ TrainingCSVGenerator está depreciado. Use glue_job_spark.py para geração de CSV com Spark.")
        self._fieldnames = self._get_csv_fieldnames()

    def generate_csv(self, conversations: List[Dict[str, Any]]) -> str:
        """
        Gera CSV formatado para treinamento

        ⚠️ DEPRECIADO: Use Spark DataFrame.write.csv() em glue_job_spark.py

        Args:
            conversations: Lista de conversas processadas

        Returns:
            Conteúdo CSV como string
        """
        logger.warning("⚠️ generate_csv() está depreciado. Migre para processamento Spark.")
        if not conversations:
            return self._generate_empty_csv()

        output = io.StringIO()

        try:
            writer = csv.DictWriter(
                output,
                fieldnames=self._fieldnames,
                extrasaction='ignore'
            )
            writer.writeheader()

            for conversation in conversations:
                row = self._prepare_csv_row(conversation)
                writer.writerow(row)

            logger.info(f"📄 CSV gerado com {len(conversations)} registros")
            return output.getvalue()

        except Exception as e:
            logger.error(f"❌ Erro ao gerar CSV: {e}")
            raise
        finally:
            output.close()

    def _prepare_csv_row(self, conversation: Dict[str, Any]) -> Dict[str, Any]:
        """Prepara uma linha do CSV garantindo todos os campos"""
        row = {}

        for field in self._fieldnames:
            if field in ['session_id', 'agent_name', 'model_provider', 'model_id']:
                row[field] = conversation.get(field, '')
            else:
                row[field] = conversation.get(field, 0)

        return row

    def _generate_empty_csv(self) -> str:
        """Gera CSV vazio quando não há dados"""
        return "session_id,message\nno_data,Nenhuma conversa encontrada"

    def _get_csv_fieldnames(self) -> List[str]:
        """Define os campos do CSV"""
        return [
            'session_id',
            'agent_name',
            'model_provider',
            'model_id',
            'total_runs',
            # Métricas de tokens
            'avg_total_tokens',
            'max_total_tokens',
            'token_explosion_ratio',
            'output_explosion_ratio',
            'token_coefficient_variation',
            'token_spike_frequency',
            'max_token_growth_rate',
            'explosive_growth_events',
            'extreme_token_flag',
            'mega_explosion_flag',
            # Métricas de tempo
            'avg_execution_time',
            'max_execution_time',
            'timeout_frequency',
            'extreme_time_frequency',
            'execution_time_cv',
            'time_explosion_events',
            'infinite_loop_risk_score',
            'timeout_flag',
            'extreme_duration_flag',
            # Métricas de tool calls
            'avg_tool_calls',
            'max_tool_calls',
            'total_tool_calls',
            'tool_explosion_ratio',
            'excessive_tools_frequency',
            'duplicate_tools_score',
            'tool_chaos_score',
            'tool_overuse_flag',
            'tool_explosion_flag',
            # Métricas temporais
            'avg_tokens_per_second',
            'min_tokens_per_second',
            'max_tokens_per_second',
            'speed_inconsistency_ratio',
            'inconsistent_speed_frequency',
            'temporal_chaos_score',
            'speed_anomaly_flag',
            # Métricas de falhas
            'success_rate',
            'failure_rate',
            'max_consecutive_failures',
            'avg_errors_per_run',
            'total_errors',
            'degradation_score',
            'systematic_failure_flag',
            'complete_breakdown_flag'
        ]
