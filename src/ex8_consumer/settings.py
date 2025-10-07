
import os
# from dotenv import load_dotenv

# load_dotenv("/home/bait/dev/ex7-consumer/.env")

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("DB_NAME")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME= os.getenv("S3_BUCKET_NAME")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")

SCHEMA_PATH = "src/ex8_consumer/schemas/reclamacoes.avsc"

SCHEMA = {
    "ano": "int",
    "trimestre": "string",
    "categoria": "string",
    "tipo": "string",
    "cnpj_if": "string",
    "instituicao_financeira": "string",
    "indice": "string",
    "quantidade_de_reclamacoes_reguladas_procedentes": "int",
    "quantidade_de_reclamacoes_reguladas_outras": "int",
    "quantidade_de_reclamacoes_nao_reguladas": "int",
    "quantidade_total_de_reclamacoes": "int",
    "quantidade_total_de_clientes_ccs_e_scr": "int",
    "quantidade_de_clientes_ccs": "int",
    "quantidade_de_clientes_scr": "int"
}
