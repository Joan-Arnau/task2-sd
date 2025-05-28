# Global Configuration File

# RabbitMQ Configuration
RABBITMQ_HOST = 'localhost'
RABBITMQ_QUEUE = 'texts_to_filter_queue'
RABBITMQ_USER = 'guest'
RABBITMQ_PASS = 'guest'

# AWS Lambda Configuration
LAMBDA_FUNCTION_NAME = 'InsultFilterWorkerLambda'
AWS_REGION = 'us-east-1'

# Insults List
INSULTS_LIST = [
    "tonto", "lleig", "boig", "idiota", "estúpid", "inútil", "desastre",
    "fracassat", "covard", "mentider", "beneit", "capsigrany", "ganàpia",
    "nyicris", "gamarús", "bocamoll", "murri", "dropo", "bleda", "xitxarel·lo"
]

# AWS S3 Configuration (for Lithops exercises)
S3_BUCKET_NAME = 'joanarnau-lithops' # Using a single bucket for input and output
S3_INPUT_PREFIX = 'input/'
S3_OUTPUT_PREFIX = 'output/'
