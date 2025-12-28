from db import get_conn

def main():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            result = cur.fetchone()
            print("DB OK, SELECT 1 =", result[0])
    finally:
        conn.close()

if __name__ == "__main__":
    main()
