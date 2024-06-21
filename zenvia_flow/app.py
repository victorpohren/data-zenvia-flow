import pandas as pd
from datetime import datetime
import os
from zenvia_flow.resources.config import URL, HEADERS, PARAMS
from zenvia_flow.utils.sqlServerConnector import pushToSqlServer, slackAlerta
from zenvia_flow.utils.zenvia_utils import zenviaAPI


def run():
    # Get report data
    try:
        all_report_data = zenviaAPI.get_report_data(URL, headers=HEADERS, params=PARAMS)
        print("Data extracted")
    except Exception as e:
        raise e
        slackAlerta(f"""
        URGENTE VERIFICAR:
        ERRO: Erro ao extrair
        ETL: data-zenvia-flow
        {datetime.now()}
        DETALHE ERRO: {e}
        """)
        os._exit(1)
    df = pd.DataFrame(all_report_data)
    # Transformed dataframe that will be pushed to SQL Server
    df_push = zenviaAPI.transformation(df)
    print("Treated data")
    df_push.to_csv("prod.csv", index=False)
    pushToSqlServer(df_push)
    
run()



