import duckdb
import streamlit as st
import pandas as pd
from queries import *  # Query SQL predefinida
from function import *
from connection import *  # Função para conectar ao DuckDB



# Configuração do título do app
st.title("Gráfico de Faturamento Diário")

# Configurações no menu lateral
st.sidebar.header("Configurações")

try:
    # Conectando ao banco de dados
    conn = connect_to_db(duckdb_file)

    # Executando a query para obter o faturamento diário
    df = conn.execute(graf_linha_faturamento).fetchdf()
    conn.close()

    # Convertendo a coluna de data para o formato datetime
    if "DATA" in df.columns and "FATURAMENTO_DIARIO" in df.columns:
        # Convertendo a coluna de data para 'date' explicitamente
        df['DATA'] = pd.to_datetime(df['DATA']).dt.date

        # Adicionando um slider para filtrar por data
        min_date = df['DATA'].min()  # agora é do tipo date
        max_date = df['DATA'].max()  # agora é do tipo date

        st.sidebar.write("### Filtro de Data")
        start_date, end_date = st.sidebar.slider(
            "Selecione o intervalo de datas:",
            min_value=min_date,
            max_value=max_date,
            value=(min_date, max_date),
            format="DD/MM/YYYY"
        )

        # Garantir que start_date e end_date sejam do tipo 'date'
        start_date = pd.to_datetime(start_date).date()  # Para garantir que seja um 'date'
        end_date = pd.to_datetime(end_date).date()  # Para garantir que seja um 'date'

        # Filtrando o DataFrame com base no intervalo de datas selecionado
        df_filtered = df[(df['DATA'] >= start_date) & (df['DATA'] <= end_date)]

        # Exibindo os dados filtrados
        if not df_filtered.empty:
            st.write(f"### Dados de Faturamento Diário ({start_date} a {end_date})", df_filtered)

            # Criando o gráfico de linha
            st.line_chart(
                df_filtered.set_index('DATA')['FATURAMENTO_DIARIO'],
                height=400,
                use_container_width=True
            )
        else:
            st.warning("Nenhum dado encontrado para o intervalo selecionado.")
    else:
        st.error("A query não retornou as colunas esperadas ('DATA' e 'FATURAMENTO_DIARIO').")

except Exception as e:
    st.error(f"Erro ao processar o banco de dados: {e}")
