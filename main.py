import oracledb as odb
import duckdb
import pandas as pd
import schedule
import time
from connection import *
from function import *
from queries import *

# Dicionário com frequências e suas respectivas configurações
config_tabelas = {
    "vendas": {"frequencia": 1, "query": vendas},
    "data_rca": {"frequencia": 5, "query": data_rca},
    "devol": {"frequencia": 10, "query": devol},
    "devol_avulsa": {"frequencia": 15, "query": devol_avulsa},
    "vendas_completa": {"frequencia": 30, "query": vendas_completa},
}
def processar_tabela(nome_tabela, func_processar_dados, query):

    # Abrir conexão com o DuckDB
    con = duckdb.connect(database=duckdb_file, read_only=False)
    
    # Executar a query para carregar os dados
    df = func_processar_dados(query)
    temp_table_name = f"temp_{nome_tabela}"
    df = df.drop_duplicates()
    con.register(temp_table_name, df)
    
    # Verificar se a tabela já existe
    if not con.execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{nome_tabela}'").fetchall():
        # Criar a tabela e inserir os dados iniciais
        con.execute(f"CREATE TABLE {nome_tabela} AS SELECT * FROM {temp_table_name}")
    else:
        # Selecionar apenas os dados que não estão na tabela destino
        cols = ", ".join(f'"{col}"' for col in df.columns)  # Proteger nomes de colunas
        primary_key_condition = " AND ".join(f"{nome_tabela}.{col} = {temp_table_name}.{col}" for col in df.columns)
        query_insercao = f"""
            SELECT {cols} 
            FROM {temp_table_name}
            WHERE NOT EXISTS (
                SELECT 1 
                FROM {nome_tabela}
                WHERE {primary_key_condition}
            )
        """
        # Inserir os dados não duplicados
        con.execute(f"""
            INSERT INTO {nome_tabela} ({cols})
            {query_insercao}
        """)
    
    # Contar o número de linhas e exibir o resultado
    num_linhas = df.shape[0]
    print(f"A tabela '{nome_tabela}' tem {num_linhas} linhas.")
    
    # Desregistrar a tabela temporária após o uso
    con.unregister(temp_table_name)
    
    # Fechar a conexão
    con.close()


# Agendar tarefas com base nas configurações
for nome_tabela, config in config_tabelas.items():
    frequencia = config["frequencia"]
    query = config["query"]
    schedule.every(frequencia).minutes.do(
        lambda nome_tabela=nome_tabela, query=query: processar_tabela(nome_tabela, processar_dados, query)
    )

print("Iniciando o agendamento de tarefas...")
while True:
    schedule.run_pending()
    time.sleep(1)