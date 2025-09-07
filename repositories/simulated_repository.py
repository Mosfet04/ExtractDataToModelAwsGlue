"""
Repositório simulado para testes
"""
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from interfaces.abstractions import IConversationRepository

logger = logging.getLogger(__name__)


class SimulatedConversationRepository(IConversationRepository):
    """Repositório simulado para testes locais"""

    def find_by_date_range(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Gera dados simulados para um intervalo de datas"""
        logger.warning("⚠️ Usando dados simulados")

        conversations = []
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')

        current_date = start
        conversation_counter = 1

        while current_date <= end:
            timestamp = int(current_date.timestamp())

            # Gerar 2-3 conversas por dia
            for i in range(1, 4):
                conversations.append({
                    '_id': f'sim_{current_date.strftime("%Y%m%d")}_{i:03d}',
                    'session_id': f'session_{current_date.strftime("%Y%m%d")}_{i:03d}',
                    'created_at': timestamp + (i * 3600),  # Espaçar por horas
                    'agent_data': {
                        'name': f'TestAgent{i}',
                        'model': {
                            'provider': 'openai' if i % 2 == 0 else 'anthropic',
                            'id': 'gpt-4' if i % 2 == 0 else 'claude-3'
                        }
                    },
                    'user_id': f'user_{conversation_counter}',
                    'memory': {
                        'runs': self._generate_sample_runs(i)
                    }
                })
                conversation_counter += 1

            current_date += timedelta(days=1)

        logger.info(f"📊 Geradas {len(conversations)} conversas simuladas")
        return conversations

    def _generate_sample_runs(self, variation: int) -> List[Dict[str, Any]]:
        """Gera runs simulados com variação"""
        runs = []

        for run_idx in range(1, variation + 2):
            runs.append({
                'status': 'RUNNING' if run_idx % 2 == 0 else 'COMPLETED',
                'content': f'Resposta simulada {run_idx}',
                'metrics': {
                    'total_tokens': [100 + (run_idx * 50)],
                    'input_tokens': [50 + (run_idx * 20)],
                    'output_tokens': [50 + (run_idx * 30)],
                    'time': [1.5 + (run_idx * 0.5)]
                },
                'tools': [f'tool_{i}' for i in range(run_idx)],
                'events': [f'event_{i}' for i in range(run_idx)]
            })

        return runs

    def close_connection(self) -> None:
        """Não há conexão para fechar"""
        pass
