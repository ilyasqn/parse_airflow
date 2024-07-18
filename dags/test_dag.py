import datetime
import requests
import json
import pandas as pd
import urllib3


from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup


def parse_task():
    urllib3.disable_warnings()

    url = 'https://kurs.kz/'

    response = requests.get(url, verify=False)
    soup = BeautifulSoup(response.content, 'html.parser', from_encoding='utf-8')

    currency_1 = 'USD'
    currency_2 = 'EUR'
    currency_3 = 'RUB'

    scripts = soup.find_all('script')
    data_list = []

    for script in scripts: #finding script
        if 'var punkts' in script.text: #finding where data is located by the given variable
            script_text = script.string
            json_str = script_text.split('var punkts = ')[1].split(';')[0]  #taking all strings and divide each by own lines
            punkts_value = json.loads(json_str)

            for punkt in punkts_value:
                rates = punkt['data'] #taking rates
                data_list.append(
                    {'name': punkt['name'], currency_1: min(rates[currency_1]) if min(rates[currency_1]) > 0 else None,
                     currency_2: min(rates[currency_2]) if min(rates[currency_2]) > 0 else None,
                     currency_3: min(rates[currency_3]) if min(rates[currency_3]) > 0 else None,
                     currency_1 + '_max': max(rates[currency_1]), currency_2 + '_max': max(rates[currency_2]),
                     currency_3 + '_max': max(rates[currency_3])})  #adding min and max rates separetely (because rates in the list)
        else:
            continue

    df = pd.DataFrame(data_list)

    min_value = df[[currency_1, currency_2, currency_3]].min().min()
    min_currency = df[[currency_2, currency_2, currency_3]].min().idxmin()
    min_exchange = df.loc[df[min_currency] == min_value, 'name'].values[0]

    max_value = df[[currency_1 + '_max', currency_2 + '_max', currency_3 + '_max']].max().max()
    max_currency = df[[currency_1 + '_max', currency_2 + '_max', currency_3 + '_max']].max().idxmax()
    max_exchange = df.loc[df[max_currency] == max_value, 'name'].values[0]

    max_currency = max_currency.split('_')[0]

    return min_value, min_currency, min_exchange, max_value, max_currency, max_exchange

def get_rates():
    min_value, min_currency, min_exchange, max_value, max_currency, max_exchange = parse_task()
    print(f"min: {min_value} {min_currency} у {min_exchange}, max: {max_value} {max_currency} у {max_exchange}")






with DAG(
    dag_id="parse_task_dag",
    start_date=datetime.datetime(2024, 7, 16),
    schedule_interval="@once",
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="parse_task",
        python_callable=get_rates
    )