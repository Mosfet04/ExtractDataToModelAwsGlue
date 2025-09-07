"""
Service refatorado para extração de conversas
Implementa SOLID principles e Clean Code
"""
import logging
from typing import List, Dict, Any

from interfaces.abstractions import IConversationExtractor, IConversationRepository
from calculators.refactored_calculators import (
    TokenMetricsCalculator, ExecutionTimeCalculator, ToolCallMetricsCalculator
)
from utils.validators import DateValidator

logger = logging.getLogger(__name__)


class ConversationExtractionService(IConversationExtractor):
    """
    Service para extração de conversas com features para treinamento
    Segue o Single Responsibility Principle
    """

    def __init__(self, repository: IConversationRepository):
        self._repository = repository
        self._token_calculator = TokenMetricsCalculator()
        self._time_calculator = ExecutionTimeCalculator()
        self._tool_calculator = ToolCallMetricsCalculator()

    def extract(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Extrai conversas por intervalo de datas com features calculadas

        Args:
            start_date: Data inicial (YYYY-MM-DD)
            end_date: Data final (YYYY-MM-DD)

        Returns:
            Lista de conversas processadas com features
        """
        # Validar datas
        validation_error = DateValidator.validate_date_range(start_date, end_date)
        if validation_error:
            raise ValueError(validation_error)

        logger.info(f"🔍 Extraindo conversas entre {start_date} e {end_date}")

        try:
            # Buscar conversas brutas
            raw_conversations = self._repository.find_by_date_range(start_date, end_date)

            if not raw_conversations:
                logger.warning(f"⚠️ Nenhuma conversa encontrada para {start_date} - {end_date}")
                return []

            # Processar cada conversa
            processed_conversations = []
            for doc in raw_conversations:
                try:
                    features = self._extract_conversation_features(doc)
                    if features:
                        processed_conversations.append(features)
                except Exception as e:
                    logger.error(f"Erro ao processar documento {doc.get('_id')}: {e}")
                    continue

            logger.info(f"📊 Conversas processadas: {len(processed_conversations)}")
            return processed_conversations

        except Exception as e:
            logger.error(f"❌ Erro na extração: {e}")
            raise
        finally:
            self._repository.close_connection()

    def _extract_conversation_features(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Extrai features de uma conversa individual"""
        try:
            # Dados básicos
            features = self._extract_basic_features(doc)

            # Extrair runs
            runs = self._extract_runs_data(doc)
            if not runs:
                logger.warning(f"Documento {doc.get('_id')} sem runs válidos")
                return features

            features['total_runs'] = len(runs)

            # Calcular métricas agregadas
            features.update(self._calculate_all_metrics(runs))

            # Adicionar timestamp formatado
            self._add_formatted_timestamps(features)

            return features

        except Exception as e:
            logger.error(f"Erro ao extrair features: {e}")
            return None

    def _extract_basic_features(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Extrai dados básicos da conversa"""
        agent_data = doc.get('agent_data', {})
        model_data = agent_data.get('model', {})

        return {
            'session_id': str(doc.get('session_id', '')),
            'created_at': doc.get('created_at', 0),
            'agent_name': agent_data.get('name', ''),
            'model_provider': model_data.get('provider', ''),
            'model_id': model_data.get('id', ''),
            'user_id': str(doc.get('user_id', '')),
            'total_runs': 0
        }

    def _extract_runs_data(self, doc: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extrai dados dos runs da conversa"""
        memory = doc.get('memory', {})
        runs = memory.get('runs', [])

        if not runs:
            return []

        run_metrics = []
        for run in runs:
            run_info = self._extract_single_run_metrics(run)
            if run_info:
                run_metrics.append(run_info)

        return run_metrics

    def _extract_single_run_metrics(self, run: Dict[str, Any]) -> Dict[str, Any]:
        """Extrai métricas de um run individual"""
        try:
            metrics = run.get('metrics', {})

            # Normalizar tokens (podem estar em arrays)
            total_tokens = self._normalize_metric_value(metrics.get('total_tokens', [0]))
            input_tokens = self._normalize_metric_value(metrics.get('input_tokens', [0]))
            output_tokens = self._normalize_metric_value(metrics.get('output_tokens', [0]))
            execution_time = self._normalize_metric_value(metrics.get('time', [0]))

            # Determinar sucesso
            success = self._determine_run_success(run)

            # Contar tool calls
            tool_calls_count = len(run.get('tools', []))

            # Detectar timeout
            has_timeout = self._detect_timeout(run)

            # Contar erros
            error_count = self._count_errors(run)

            return {
                'total_tokens': total_tokens,
                'input_tokens': input_tokens,
                'output_tokens': output_tokens,
                'execution_time': execution_time,
                'success': success,
                'tool_calls_count': tool_calls_count,
                'error_count': error_count,
                'has_timeout': has_timeout
            }
        except Exception as e:
            logger.error(f"Erro ao processar run: {e}")
            return None

    def _normalize_metric_value(self, value):
        """Normaliza valores que podem estar em arrays"""
        if isinstance(value, list):
            return value[0] if value else 0
        return value or 0

    def _determine_run_success(self, run: Dict[str, Any]) -> bool:
        """Determina se um run foi bem-sucedido"""
        status = run.get('status', '')
        events = run.get('events', [])

        return (
            status == 'RUNNING' or
            'RunCompleted' in str(events)
        )

    def _detect_timeout(self, run: Dict[str, Any]) -> bool:
        """Detecta se houve timeout no run"""
        events = run.get('events', [])
        return any('timeout' in str(event).lower() for event in events)

    def _count_errors(self, run: Dict[str, Any]) -> int:
        """Conta erros no run"""
        events = run.get('events', [])
        return sum(1 for event in events if 'error' in str(event).lower())

    def _calculate_all_metrics(self, runs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calcula todas as métricas usando os calculadores especializados"""
        metrics = {}

        # Métricas de tokens
        metrics.update(self._token_calculator.calculate(runs))

        # Métricas de tempo
        metrics.update(self._time_calculator.calculate(runs))

        # Métricas de tool calls
        metrics.update(self._tool_calculator.calculate(runs))

        # Métricas de consistência temporal
        metrics.update(self._calculate_temporal_consistency(runs))

        # Métricas de falhas
        metrics.update(self._calculate_failure_metrics(runs))

        return metrics

    def _calculate_temporal_consistency(self, runs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calcula métricas de consistência temporal"""
        if not runs:
            return self._get_empty_temporal_metrics()

        tokens_per_second = []
        for run in runs:
            time = run.get('execution_time', 0)
            tokens = run.get('total_tokens', 0)

            if time > 0:
                tps = tokens / time
                tokens_per_second.append(tps)

        if not tokens_per_second:
            return self._get_empty_temporal_metrics()

        import numpy as np

        avg_tps = np.mean(tokens_per_second)
        min_tps = np.min(tokens_per_second)
        max_tps = np.max(tokens_per_second)

        speed_inconsistency_ratio = max_tps / min_tps if min_tps > 0 else 0

        # Frequência de velocidade inconsistente
        slow_runs = sum(1 for tps in tokens_per_second if tps < 50)
        fast_runs = sum(1 for tps in tokens_per_second if tps > 1000)
        inconsistent_speed_frequency = (slow_runs + fast_runs) / len(tokens_per_second)

        return {
            'avg_tokens_per_second': avg_tps,
            'min_tokens_per_second': min_tps,
            'max_tokens_per_second': max_tps,
            'speed_inconsistency_ratio': speed_inconsistency_ratio,
            'inconsistent_speed_frequency': inconsistent_speed_frequency,
            'temporal_chaos_score': inconsistent_speed_frequency,
            'speed_anomaly_flag': 1 if speed_inconsistency_ratio > 10 else 0
        }

    def _calculate_failure_metrics(self, runs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calcula métricas de falhas"""
        if not runs:
            return self._get_empty_failure_metrics()

        successes = [run.get('success', False) for run in runs]
        errors = [run.get('error_count', 0) for run in runs]

        success_rate = sum(successes) / len(successes)
        failure_rate = 1 - success_rate

        # Falhas consecutivas
        max_consecutive_failures = self._calculate_max_consecutive_failures(successes)

        # Métricas de erro
        total_errors = sum(errors)
        avg_errors_per_run = total_errors / len(errors)

        return {
            'success_rate': success_rate,
            'failure_rate': failure_rate,
            'max_consecutive_failures': max_consecutive_failures,
            'avg_errors_per_run': avg_errors_per_run,
            'total_errors': total_errors,
            'degradation_score': 0,  # Simplificado
            'systematic_failure_flag': 1 if failure_rate > 0.5 else 0,
            'complete_breakdown_flag': 1 if success_rate < 0.1 else 0
        }

    def _calculate_max_consecutive_failures(self, successes: List[bool]) -> int:
        """Calcula máximo de falhas consecutivas"""
        consecutive_failures = []
        current_consecutive = 0

        for success in successes:
            if not success:
                current_consecutive += 1
            else:
                if current_consecutive > 0:
                    consecutive_failures.append(current_consecutive)
                current_consecutive = 0

        if current_consecutive > 0:
            consecutive_failures.append(current_consecutive)

        return max(consecutive_failures) if consecutive_failures else 0

    def _add_formatted_timestamps(self, features: Dict[str, Any]) -> None:
        """Adiciona timestamps formatados"""
        if features.get('created_at'):
            from datetime import datetime
            timestamp = features['created_at']
            features['date'] = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            features['timestamp'] = datetime.fromtimestamp(timestamp).isoformat()

    def _get_empty_temporal_metrics(self) -> Dict[str, Any]:
        """Retorna métricas temporais vazias"""
        return {
            'avg_tokens_per_second': 0,
            'min_tokens_per_second': 0,
            'max_tokens_per_second': 0,
            'speed_inconsistency_ratio': 0,
            'inconsistent_speed_frequency': 0,
            'temporal_chaos_score': 0,
            'speed_anomaly_flag': 0
        }

    def _get_empty_failure_metrics(self) -> Dict[str, Any]:
        """Retorna métricas de falha vazias"""
        return {
            'success_rate': 0,
            'failure_rate': 1,
            'max_consecutive_failures': 0,
            'avg_errors_per_run': 0,
            'total_errors': 0,
            'degradation_score': 0,
            'systematic_failure_flag': 1,
            'complete_breakdown_flag': 1
        }
