import os

S3_BUCKET = os.getenv("S3_BUCKET", "football-datalake")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL") or None
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

PG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "dbname": os.getenv("PG_DB", "football"),
    "user": os.getenv("PG_USER", "football"),
    "password": os.getenv("PG_PASSWORD", "football"),
}


def jdbc_url() -> str:
    return f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}"
