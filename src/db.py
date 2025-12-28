import os
import psycopg2
from dotenv import load_dotenv

def get_conn():
    # Charge les variables depuis .env si présent
    load_dotenv()

    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    dbname = os.getenv("PGDATABASE")

    if not all([user, password, dbname]):
        raise ValueError("PGUSER/PGPASSWORD/PGDATABASE doivent être définis (via .env).")

    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
    )
