import pyodbc as odbc
import pandas as pd
import json
from zenvia_flow.secrets.get_token import get_secret
from zenvia_flow.resources.config import *
import time
from io import StringIO
from datetime import datetime
import time
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import slack


def slackAlerta(msg):
    token = token
    client = slack.WebClient(token=token)
    client.chat_postMessage(channel='alertas_engenharia',text=msg)

def sqlConnector():
    secret = secret
    server = server
    database = database
    username = username
    password = password
    conn = conn
    quoted = quoted
    new_con = new_con

    return create_engine(new_con, fast_executemany=True)

def pushToSqlServer(df):
    
    #df = df.replace({r'[^\x00-\u017F]+': ''}, regex=True)
    engine = sqlConnector()
    table_name = 'relatorio_chamadas'
    schema = 'zenvia'
    inserted_at = 'inserted_at'
    df_cols = pd.DataFrame(columns = ['id','data_criacao','ramal_id_origem','destino_numero','destino_status','destino_duracao_segundos','destino_duracao_falada_segundos', 'ramal_origem_login', 'url_gravacao'])
    df_data = pd.concat([df_cols,df],ignore_index=True,join='inner')
    df_data.rename({'id': 'id', 'data_criacao': 'dt_criacao', 'ramal_id_origem': 'id_ramal_origem', 'destino_numero': 'destino_numero', 'destino_status': 'destino_status', 'destino_duracao_segundos': 'destino_duracao_segundos', 'destino_duracao_falada_segundos': 'destino_duracao_falada_segundos', 'ramal_origem_login': 'ramal_origem_login', 'url_gravacao': 'url_gravacao'},axis=1, inplace=True)
    df_data = df_data.fillna(pd.NA)
    df_data = df_data.replace('None', pd.NA)
    df_data[f'{inserted_at}'] = datetime.now()
    conn = engine.connect()

    for i in range(3):
        try:
            engine = sqlConnector()
            error_content = None
            df_data.to_sql(name=table_name, index=False, con=engine, schema=schema, if_exists='append',method='multi',chunksize=((2100//len(df_data.columns))-1))
            print(f'inserted into {schema}.{table_name}')
            break
        except Exception as e:
            ###raise e
            error_content = e
    if error_content is not None:
        slackAlerta(f"""
        URGENTE VERIFICAR:
        ERRO: Erro ao inserir Server - {schema}.{table_name}
        ETL: data-zenvia-flow
        {datetime.now()}
        DETALHE ERRO: {e}
        """)
        return

    for i in range(3):
        try:
            engine = sqlConnector()
            conn = engine.connect()
            conn.execute(text(f"""with cte as (select id,max(inserted_at) max_data
                        from zenvia.relatorio_chamadas
                        group by id)
                        delete t
                        from zenvia.relatorio_chamadas t
                        inner join cte
                        on  (t.id = cte.id)
                        and (t.inserted_at <> cte.max_data)"""))
            conn.commit()
            conn.close()
            print(f'dedudplicated {table_name}')
            return
        except Exception as e:
            ##raise e
            error_content = e
    if error_content is not None:
        slackAlerta(f"""
        URGENTE VERIFICAR:
        ERRO: Erro ao deletar duplicados Server - {schema}.{table_name}
        ETL: data-zenvia-flow
        {datetime.now()}
        DETALHE ERRO: {error_content}
        """)