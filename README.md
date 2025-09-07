# 🚀 AWS Glue Job - ExtractDataToModel

## 📋 Resumo

Esta é a versão adaptada para AWS Glue do código original de extração de dados do MongoDB, processamento para treinamento de Machine Learning e upload para S3.

## 🏗️ Arquitetura

### 📁 Estrutura
```
📦 ExtractDataToModelAwsGlue/
├── 📄 glue_job.py                    # ← SCRIPT PRINCIPAL DO GLUE
├── 📁 config/
│   └── constants.py                         # Constantes centralizadas
├── 📁 interfaces/
│   └── abstractions.py                     # Contratos e interfaces
├── 📁 repositories/
│   ├── mongo_repository.py                 # Implementação MongoDB
│   └── simulated_repository.py             # Implementação simulada
├── 📁 services/
│   ├── conversation_extraction_service.py  # Extração de conversas
│   ├── csv_generator_service.py           # Geração de CSV
│   └── s3_upload_service.py               # Upload S3
├── 📁 calculators/
│   └── refactored_calculators.py          # Calculadores especializados
├── 📁 factories/
│   └── repository_factory.py              # Factory pattern
└── 📁 utils/
    └── validators.py                       # Validações
```

## 🚀 Como Usar

### 1. Criar Glue Job no AWS Console

1. Acesse AWS Glue Console
2. Crie um novo Glue Job
3. Selecione "Python Shell" como tipo
4. Faça upload do código ou use S3
5. Configure as variáveis de ambiente

### 2. Variáveis de Ambiente

```bash
MONGO_URI=mongodb://your-mongodb-uri
DATABASE_NAME=conversations
COLLECTION_NAME=chat_data
S3_BUCKET=training-data-bucket
START_DATE=2024-12-01
END_DATE=2024-12-31
```

### 3. Executar o Job

O job pode ser executado manualmente ou agendado via Triggers do Glue.

## 🔧 Configuração do Glue Job

### Parâmetros do Job
- **Tipo**: Python Shell
- **Python Version**: Python 3.9
- **Worker Type**: Standard
- **Number of Workers**: 1 (suficiente para este workload)
- **Timeout**: 15 minutos
- **Max Retries**: 3

### Dependências
As bibliotecas necessárias estão listadas em `requirements-minimal.txt`:
- boto3
- pymongo
- numpy
- certifi

### IAM Role
O Glue Job precisa de uma IAM Role com as seguintes permissões:
- Acesso ao MongoDB (se não estiver na VPC)
- Acesso ao S3 bucket
- Glue service role permissions

## 📊 Saída

O job gera um arquivo CSV no S3 com as seguintes colunas:
- session_id
- agent_name
- model_provider
- model_id
- total_runs
- Métricas de tokens (avg, max, ratios, etc.)
- Métricas de tempo de execução
- Métricas de tool calls
- Métricas de falhas
- Métricas temporais

## 🔍 Logs

Os logs são enviados para CloudWatch Logs no grupo:
`/aws-glue/jobs/extract-conversations-job`

## ⚠️ Considerações

1. **Performance**: Para grandes volumes de dados, considere usar Glue ETL jobs com Spark
2. **Custos**: Python Shell jobs têm custos baseados na duração
3. **Limitações**: Python Shell tem limite de 15 minutos por execução
4. **Rede**: Se MongoDB estiver em VPC, configure VPC access para o Glue job

## 🧪 Teste Local

Para testar localmente:

```python
python glue_job.py
```

Certifique-se de ter as variáveis de ambiente configuradas.
