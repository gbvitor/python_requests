import oracledb
import os
from dotenv import load_dotenv

# Carrega as configurações do arquivo .env
load_dotenv()

def connect_to_oracle():
    """Estabelece conexão usando variáveis de ambiente"""
    try:
        dsn = oracledb.makedsn(
            os.getenv("DB_HOST"), 
            os.getenv("DB_PORT"), 
            service_name=os.getenv("DB_SERVICE_NAME")
        )
        return oracledb.connect(
            user=os.getenv("DB_USERNAME"), 
            password=os.getenv("DB_PASSWORD"), 
            dsn=dsn
        )
    except oracledb.Error as e:
        print(f"Erro na conexão: {e}")
        return None

def execute_query(connection, query):
    """Executa e exibe os resultados de uma consulta"""
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            print(row)
        cursor.close()
    except oracledb.Error as e:
        print(f"Erro na consulta: {e}")

if __name__ == "__main__":
    conn = connect_to_oracle()
    if conn:
        print("Conexão estabelecida!")
        execute_query(conn, "SELECT * FROM AD_TGCONAGRO")
        conn.close()
        print("Conexão fechada.")
