#!/usr/bin/env python3
"""
Script de teste rápido para o Glue Job de extração de conversas por data
"""
import json
import os
import sys

# Configurar ambiente de teste
os.environ['MONGO_URI'] = 'mongodb://localhost:27017'
os.environ['DATABASE_NAME'] = 'conversations'
os.environ['COLLECTION_NAME'] = 'chat_data'
os.environ['S3_BUCKET'] = 'training-data-bucket'

# Importar função Glue
sys.path.append(os.path.dirname(__file__))
from glue_job import main

def test_date_range_extraction():
    """Teste com intervalo de datas"""

    print("=" * 60)
    print("🧪 TESTE: Extração de conversas por intervalo de datas (Glue)")
    print("=" * 60)

    # Configurar datas de teste
    os.environ['START_DATE'] = '2024-12-20'
    os.environ['END_DATE'] = '2024-12-25'

    print(f"📅 Testando extração para período: {os.environ['START_DATE']} até {os.environ['END_DATE']}")

    try:
        main()

        print("\n✅ Glue Job executado com sucesso!")
        print("📊 Verifique os logs acima para detalhes da execução")

    except Exception as e:
        print(f"\n❌ Erro durante execução: {e}")
        import traceback
        traceback.print_exc()

def test_without_dates():
    """Teste sem datas específicas (usa valores padrão)"""

    print("\n" + "=" * 60)
    print("🧪 TESTE: Extração com datas padrão")
    print("=" * 60)

    # Remover variáveis de ambiente para usar valores padrão
    if 'START_DATE' in os.environ:
        del os.environ['START_DATE']
    if 'END_DATE' in os.environ:
        del os.environ['END_DATE']

    print("📅 Usando datas padrão (último mês)")

    try:
        main()

        print("\n✅ Glue Job executado com sucesso!")
        print("📊 Verifique os logs acima para detalhes da execução")

    except Exception as e:
        print(f"\n❌ Erro durante execução: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🚀 Iniciando testes do Glue Job...")

    # Teste 1: Com intervalo específico
    test_date_range_extraction()

    # Teste 2: Com datas padrão
    test_without_dates()

    print("\n" + "=" * 60)
    print("🏁 Todos os testes concluídos!")
    print("=" * 60)