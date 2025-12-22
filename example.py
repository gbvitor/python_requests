import oracledb


def connect_to_oracle(username, password, host, port, service_name):
    """
    Estabelece conexão com Oracle Database 19c
    """
    try:
        dsn = oracledb.makedsn(host, port, service_name=service_name)
        connection = oracledb.connect(user=username, password=password, dsn=dsn)
        print("Conexão estabelecida com sucesso!")
        return connection
    except oracledb.Error as error:
        print(f"Erro ao conectar ao banco de dados: {error}")
        return None


def execute_query(connection, query):
    """
    Executa uma consulta no banco de dados Oracle
    """
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()

        # Exibe os nomes das colunas
        columns = [desc[0] for desc in cursor.description]
        print("Colunas:", columns)

        # Exibe os resultados
        print("\nResultados:")
        for row in results:
            print(row)

        cursor.close()
        return results
    except oracledb.Error as error:
        print(f"Erro ao executar consulta: {error}")
        return None


if __name__ == "__main__":
    # Parâmetros de conexão
    USERNAME = "SANKHYA"
    PASSWORD = "ag5nxt73"
    HOST = "192.168.242.205"
    PORT = "12821"
    SERVICE_NAME = "orcl"

    # Conectar ao banco de dados
    conn = connect_to_oracle(USERNAME, PASSWORD, HOST, PORT, SERVICE_NAME)

    if conn:
        # Exemplo de consulta
        query = "SELECT * FROM AD_TGCONAGRO"
        execute_query(conn, query)

        # Fechar conexão
        conn.close()
        print("\nConexão fechada.")
