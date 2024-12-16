import oracledb as odb
import pandas as pd
import duckdb
from connection import *
from queries import *

def processar_dados(query):
    try:
        # Conectar ao banco de dados
        connection = odb.connect(
            user=usuario_odb,
            password=senha_odb,
            dsn=conexao_odb
        )

        # Criar um cursor
        cursor = connection.cursor()

        # Executar a consulta
        cursor.execute(query)

        # Obter todos os resultados da consulta
        consulta_odb = cursor.fetchall()

        # Obter os nomes das colunas da consulta
        colunas = [col[0] for col in cursor.description]

    except odb.DatabaseError as e:
        # Capturar erros de banco de dados e exibir uma mensagem de erro
        print(f"Erro ao acessar o banco de dados: {e}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro

    finally:
        # Certifique-se de que o cursor e a conexão sejam fechados
        try:
            cursor.close()
        except:
            pass
        try:
            connection.close()
        except:
            pass

        # Criar um DataFrame com os resultados
    df = pd.DataFrame(consulta_odb, columns=colunas)

    # Retornar o DataFrame
    return df

def connect_to_db(duckdb_file):
    
    con = duckdb.connect(duckdb_file, read_only=True)
    return con

def duck_consulta(query):
    con = connect_to_db(duckdb_file)
    
    result = con.execute(query)  # O result ainda é um objeto executável aqui
    values = result.fetchall()  # Obtém os dados como uma lista
    colunas = [desc[0] for desc in result.description]  # Obtém os nomes das colunas
    df = pd.DataFrame(values, columns=colunas)
    return df    