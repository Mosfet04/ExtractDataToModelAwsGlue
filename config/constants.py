"""
Constantes da aplicação
"""

# Thresholds para detecção de alucinações
class HallucinationThresholds:
    TOKEN_SPIKE_MULTIPLIER = 2.0
    EXPLOSIVE_GROWTH_THRESHOLD = 5.0
    EXTREME_TOKEN_LIMIT = 50000
    MEGA_EXPLOSION_LIMIT = 100000
    EXTREME_TIME_LIMIT = 30.0
    TIMEOUT_LIMIT = 60.0
    EXCESSIVE_TOOLS_LIMIT = 10
    TOOL_OVERUSE_LIMIT = 20
    SPEED_INCONSISTENCY_LIMIT = 10.0
    FAILURE_RATE_THRESHOLD = 0.5
    BREAKDOWN_THRESHOLD = 0.1

# Configurações do CSV
class CSVConfig:
    CONTENT_PREVIEW_LENGTH = 200
    DEFAULT_ENCODING = 'utf-8'
    MIME_TYPE = 'text/csv'

# Configurações do MongoDB
class MongoConfig:
    DEFAULT_HOST = 'mongodb://localhost:27017'
    DEFAULT_DATABASE = 'conversations'
    DEFAULT_COLLECTION = 'chat_data'

# Configurações do S3
class S3Config:
    DEFAULT_BUCKET = 'training-data-bucket'
    FOLDER_PREFIX = 'training_data'

# Formatos de data
class DateFormats:
    INPUT_FORMAT = '%Y-%m-%d'
    TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'
    ISO_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
