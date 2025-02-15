from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json 
from datetime import datetime, timedelta,date

## define DAG 
with DAG(
    dag_id='Bitcoin',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
    #dagrun_timeout=timedelta(minutes=5)
) as dag:
    
    @task
    def create_table():
        # step 1: create a postgres table if it doesnt exits
        postgres_hook=PostgresHook(postgres_conn_id='aws_postgres_conn_id')

        # SQL query to create the table 
        create_table_query=""" 
        CREATE TABLE if not EXISTS bitcoin_data (
        id SERIAL Primary key,
        title varchar(255),
        Digital_Currency_Code varchar(10),
        Digital_Currency_Name varchar(20),
        Market_Code varchar(3),
        Market_Name varchar(255),
        Last_Refreshed_Date Date,
        Time_Zone varchar(5),
        open_price decimal,
        high_price decimal,
        low_price decimal,
        close_price decimal
        );
        """

        postgres_hook.run(create_table_query)

    # step 2: extract the bitcoin data from API
   

    extract_btc=SimpleHttpOperator(
        task_id='extract_btc',
        http_conn_id='alpha_vantage_api',
        endpoint='query',
        method='GET',
        data={
            'function':"{{conn.alpha_vantage_api.extra_dejson.function}}",
            'symbol':"{{conn.alpha_vantage_api.extra_dejson.symbol}}",
            'market':"{{conn.alpha_vantage_api.extra_dejson.market}}",
            'apikey':"{{conn.alpha_vantage_api.extra_dejson.apikey}}"
        },
        response_filter=lambda response:response.json(),
    )

    @task 
    def transform_btc_data(response):
        dates = list(response['Time Series (Digital Currency Daily)'].keys())
        first_date =dates[0]
        previous_date=dates[1]
        btc_data = {
        'title': response['Meta Data']['1. Information'],
        'Digital_Currency_Code': response['Meta Data']['2. Digital Currency Code'],
        'Digital_Currency_Name': response['Meta Data']['3. Digital Currency Name'],
        'Market_Code': response['Meta Data']['4. Market Code'],
        'Market_Name': response['Meta Data']['5. Market Name'],
        'Last_Refreshed_Date': response['Meta Data']['6. Last Refreshed'],
        'Time_Zone': response['Meta Data']['7. Time Zone'],
        'open_price': response['Time Series (Digital Currency Daily)'][first_date]['1. open'],
        'high_price': response['Time Series (Digital Currency Daily)'][first_date]['2. high'],
        'low_price': response['Time Series (Digital Currency Daily)'][first_date]['3. low'],
        'close_price': response['Time Series (Digital Currency Daily)'][first_date]['4. close'],  # Extract only close price
    }
        return btc_data
    
    # step 4: load the data into postgres SQL
    @task 
    def load_data_postgres(data):
        postgreshook=PostgresHook(postgres_conn_id='aws_postgres_conn_id')

        # swfine SQL insert query 
        insert_query=""" 
        insert into bitcoin_data (title,Digital_Currency_Code,Digital_Currency_Name,Market_Code,Market_Name,Last_Refreshed_Date,Time_Zone,open_price,high_price,low_price,close_price)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        postgreshook.run(insert_query,parameters=(
            data['title'],
            data['Digital_Currency_Code'],
            data['Digital_Currency_Name'],
            data['Market_Code'],
            data['Market_Name'],
            data['Last_Refreshed_Date'],
            data['Time_Zone'],
            data['open_price'],
            data['high_price'],
            data['low_price'],
            data['close_price'],
        ))

    #step 5: define the dependencies 
    # extract 
    create_table()>>extract_btc 
    api_response=extract_btc.output
    transformed_data=transform_btc_data(api_response)
    load_data_postgres(transformed_data)

