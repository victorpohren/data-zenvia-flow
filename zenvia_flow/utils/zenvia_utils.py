import pandas as pd
import requests
import json
import time
from datetime import datetime


class zenviaAPI:
    

    def send_request(url, params:dict, headers:dict, recursion_count:int = 0) -> dict:

        try:
            response = requests.get(url, params=params, headers=headers)
            data = response.json()
            time.sleep(.12)
            if data["status"] != 200: raise
            if type(data) == None: raise
            return data

        except Exception as e:
            if recursion_count == 10: raise e
            time.sleep(.5)
            recursion_count+=1
            zenviaAPI.send_request(url, params=params, headers=headers,recursion_count=recursion_count)
        
    
    def get_report_data(url, params, headers):
        all_data = []

        # Define max results per page
        results_per_page = int(params["limite"])
        
        # Pagination loop
        offset = int(params["posicao"])
        has_more_data = True
        while has_more_data:
            # Uptade offset parameter
            params["posicao"] = str(offset)
            # API request
            data = zenviaAPI.send_request(url, params=params, headers=headers)
            # Created a separete function that will allow the request to retry 5
            # times before raising an error because the api timesout easily.
                #{'status': 504, 'sucesso': False, 'motivo': 110, 'stage': 'api', 'message': 'Timeout'}
                
            # Data processing
            relatorio = data['dados']['relatorio']
            for item in relatorio:
                all_data.append(item)

            # Check the limitation of max results per page
            if len(relatorio) < results_per_page:
                has_more_data = False
            else:
                offset += results_per_page

        return all_data


    def transformation(df):

        # Extract 'ramal' from 'origem' and create separate columns
        df['ramal'] = df['origem'].apply(lambda x: x.get('ramal') if "ramal" in x else pd.NA)
        df.drop(columns=['origem'], inplace=True)

        # Extract keys from 'destino' using json_normalize
        destino_normalized = pd.json_normalize(df['destino'])
        df = pd.concat([df, destino_normalized.add_prefix('destino_')], axis=1)
        df.drop(columns=['destino'], inplace=True)

        # Extract keys from 'destino' using json_normalize
        ramal_normalized = pd.json_normalize(df['ramal'])
        df = pd.concat([df, ramal_normalized.add_prefix('ramal_origem_')], axis=1)
        df.drop(columns=['ramal'], inplace=True)
        
        # Select Columns

        # data_criacao to datetime
        df['data_criacao'] = pd.to_datetime(df['data_criacao'])

        # Create column hora_ligacao
        df['hora_ligacao'] = df['data_criacao'].dt.time

        # Format column data_criaco to 'yyyy-mm-dd'
        df['data_criacao'] = df['data_criacao'].dt.strftime('%Y-%m-%d')

        return df
