"""
Calculadores refatorados seguindo SOLID e Clean Code
"""
import logging
from typing import List, Dict, Any
import numpy as np

from interfaces.abstractions import IMetricsCalculator
from config.constants import HallucinationThresholds

logger = logging.getLogger(__name__)


class TokenMetricsCalculator(IMetricsCalculator):
    """Calcula métricas de explosão de tokens"""

    def calculate(self, run_metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calcula métricas de tokens"""
        if not run_metrics:
            return self._get_empty_metrics()

        total_tokens_list = [r.get('total_tokens', 0) for r in run_metrics]
        output_tokens_list = [r.get('output_tokens', 0) for r in run_metrics]

        if not any(total_tokens_list):
            return self._get_empty_metrics()

        avg_total = np.mean(total_tokens_list)
        max_total = np.max(total_tokens_list)
        avg_output = np.mean(output_tokens_list)
        max_output = np.max(output_tokens_list)

        # Ratios de explosão
        token_explosion_ratio = max_total / avg_total if avg_total > 0 else 1
        output_explosion_ratio = max_output / avg_output if avg_output > 0 else 1

        # Coeficiente de variação
        token_std = np.std(total_tokens_list)
        token_coefficient_variation = token_std / avg_total if avg_total > 0 else 0

        # Frequência de spikes
        spike_threshold = avg_total * HallucinationThresholds.TOKEN_SPIKE_MULTIPLIER
        spike_count = sum(1 for tokens in total_tokens_list if tokens > spike_threshold)
        token_spike_frequency = spike_count / len(total_tokens_list)

        # Taxa de crescimento máximo
        growth_rates = self._calculate_growth_rates(total_tokens_list)
        max_token_growth_rate = max(growth_rates) if growth_rates else 1

        # Eventos de crescimento explosivo
        explosive_growth_events = sum(
            1 for rate in growth_rates
            if rate > HallucinationThresholds.EXPLOSIVE_GROWTH_THRESHOLD
        )

        return {
            'avg_total_tokens': avg_total,
            'max_total_tokens': max_total,
            'token_explosion_ratio': token_explosion_ratio,
            'output_explosion_ratio': output_explosion_ratio,
            'token_coefficient_variation': token_coefficient_variation,
            'token_spike_frequency': token_spike_frequency,
            'max_token_growth_rate': max_token_growth_rate,
            'explosive_growth_events': explosive_growth_events,
            'extreme_token_flag': 1 if max_total > HallucinationThresholds.EXTREME_TOKEN_LIMIT else 0,
            'mega_explosion_flag': 1 if max_total > HallucinationThresholds.MEGA_EXPLOSION_LIMIT else 0
        }

    def _calculate_growth_rates(self, tokens_list: List[float]) -> List[float]:
        """Calcula taxas de crescimento entre execuções consecutivas"""
        growth_rates = []
        for i in range(1, len(tokens_list)):
            if tokens_list[i-1] > 0:
                growth_rate = tokens_list[i] / tokens_list[i-1]
                growth_rates.append(growth_rate)
        return growth_rates

    def _get_empty_metrics(self) -> Dict[str, Any]:
        """Retorna métricas vazias"""
        return {
            'avg_total_tokens': 0, 'max_total_tokens': 0, 'token_explosion_ratio': 0,
            'output_explosion_ratio': 0, 'token_coefficient_variation': 0,
            'token_spike_frequency': 0, 'max_token_growth_rate': 1,
            'explosive_growth_events': 0, 'extreme_token_flag': 0, 'mega_explosion_flag': 0
        }


class ExecutionTimeCalculator(IMetricsCalculator):
    """Calcula métricas de tempo de execução e detecção de loops"""

    def calculate(self, run_metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calcula métricas de tempo de execução"""
        if not run_metrics:
            return self._get_empty_metrics()

        execution_times = [r.get('execution_time', 0) for r in run_metrics]

        if not any(execution_times):
            return self._get_empty_metrics()

        avg_time = np.mean(execution_times)
        max_time = np.max(execution_times)

        # Detecção de timeouts
        timeout_count = sum(1 for run in run_metrics if run.get('has_timeout', False))
        timeout_frequency = timeout_count / len(run_metrics)

        # Tempos extremamente longos
        extreme_time_count = sum(
            1 for time in execution_times
            if time > HallucinationThresholds.EXTREME_TIME_LIMIT
        )
        extreme_time_frequency = extreme_time_count / len(execution_times)

        # Coeficiente de variação do tempo
        time_std = np.std(execution_times)
        execution_time_cv = time_std / avg_time if avg_time > 0 else 0

        # Eventos de explosão temporal
        time_increases = sum(
            1 for i in range(1, len(execution_times))
            if execution_times[i] > execution_times[i-1] * HallucinationThresholds.TOKEN_SPIKE_MULTIPLIER
        )

        # Score de risco de loop infinito
        infinite_loop_risk_score = timeout_frequency + extreme_time_frequency

        return {
            'avg_execution_time': avg_time,
            'max_execution_time': max_time,
            'timeout_frequency': timeout_frequency,
            'extreme_time_frequency': extreme_time_frequency,
            'execution_time_cv': execution_time_cv,
            'time_explosion_events': time_increases,
            'infinite_loop_risk_score': infinite_loop_risk_score,
            'timeout_flag': 1 if timeout_count > 0 else 0,
            'extreme_duration_flag': 1 if max_time > HallucinationThresholds.TIMEOUT_LIMIT else 0
        }

    def _get_empty_metrics(self) -> Dict[str, Any]:
        """Retorna métricas vazias"""
        return {
            'avg_execution_time': 0, 'max_execution_time': 0, 'timeout_frequency': 0,
            'extreme_time_frequency': 0, 'execution_time_cv': 0, 'time_explosion_events': 0,
            'infinite_loop_risk_score': 0, 'timeout_flag': 0, 'extreme_duration_flag': 0
        }


class ToolCallMetricsCalculator(IMetricsCalculator):
    """Calcula métricas de uso de ferramentas"""

    def calculate(self, run_metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calcula métricas de tool calls"""
        if not run_metrics:
            return self._get_empty_metrics()

        tool_calls_list = [r.get('tool_calls_count', 0) for r in run_metrics]

        if not any(tool_calls_list):
            return self._get_empty_metrics()

        avg_tools = np.mean(tool_calls_list)
        max_tools = np.max(tool_calls_list)
        total_tools = sum(tool_calls_list)

        # Ratio de explosão de tools
        tool_explosion_ratio = max_tools / avg_tools if avg_tools > 0 else 1

        # Frequência de uso excessivo
        excessive_count = sum(
            1 for count in tool_calls_list
            if count > HallucinationThresholds.EXCESSIVE_TOOLS_LIMIT
        )
        excessive_tools_frequency = excessive_count / len(tool_calls_list)

        # Score de chaos geral
        tool_chaos_score = excessive_tools_frequency + (tool_explosion_ratio / 10)

        return {
            'avg_tool_calls': avg_tools,
            'max_tool_calls': max_tools,
            'total_tool_calls': total_tools,
            'tool_explosion_ratio': tool_explosion_ratio,
            'excessive_tools_frequency': excessive_tools_frequency,
            'duplicate_tools_score': 0,  # Simplificado
            'tool_chaos_score': tool_chaos_score,
            'tool_overuse_flag': 1 if max_tools > HallucinationThresholds.TOOL_OVERUSE_LIMIT else 0,
            'tool_explosion_flag': 1 if tool_explosion_ratio > HallucinationThresholds.EXPLOSIVE_GROWTH_THRESHOLD else 0
        }

    def _get_empty_metrics(self) -> Dict[str, Any]:
        """Retorna métricas vazias"""
        return {
            'avg_tool_calls': 0, 'max_tool_calls': 0, 'total_tool_calls': 0,
            'tool_explosion_ratio': 0, 'excessive_tools_frequency': 0,
            'duplicate_tools_score': 0, 'tool_chaos_score': 0,
            'tool_overuse_flag': 0, 'tool_explosion_flag': 0
        }
