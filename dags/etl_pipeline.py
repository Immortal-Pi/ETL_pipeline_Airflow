from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago 
import json 
import dotenv

dotenv.load_dotenv()

## define the DAG
with DAG(
    dag_id='nasa',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False

) as dag:
    
    # step 1: create the table if it doesnt exists 
    @task 
    def create_table():
        ## initialize the postgreshook - interact with prostgres 
        postgres_hook=PostgresHook(postgres_conn_id='my_postgres_connection')
        
        ##SQL query to the table 
        create_table_query=""" 
        CREATE TABLE if not EXISTS apod_data (
        id SERIAL Primary key,
        title varchar(255),
        explanation text,
        url text,
        date Date, 
        media_type varchar(50)
        );
        """
        ## execute the table creation query 
        postgres_hook.run(create_table_query)


    # step 2: extracrt the nasa API data - astronomy picture of the day
    # https://api.nasa.gov/planetary/apod?api_key=gEyV5fw8aNYd5c0P7JS9WzFDajpbT7aBQkbeRnmR 
    # extract pipeline 
    extract_apod=SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api', ## connection ID degined in Airflow for NASA API
        endpoint='planetary/apod', ## NASA AAI endpoint 
        method='GET',
        data={"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"}, ## use api key
        response_filter=lambda response:response.json(), ## convert response to json 
    )

    # step 3: transform the data(pick the information that i need to save)
    @task
    def transform_apod_date(response):
        apod_data={
            'title':response.get('title',''),
            'explanation':response.get('explanation',''),
            'url':response.get('url',''),
            'date':response.get('date',''),
            'media_type':response.get('media_type','')

        }
        return apod_data
         
     # step 4: load the data into postgres SQL
    @task
    def load_data_postgres(apod_data):
        ## initialize the postgresHook
        postgreshook=PostgresHook(postgres_conn_id='my_postgres_connection')

        # define SQL insert query 
        insert_query=""" 
        INSERT INTO apod_data (title,explanation,url,date,media_type)
        VALUES(%s,%s,%s,%s,%s);
        """
        # execute the sql query 
        postgreshook.run(insert_query,parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type'],
        ))
        
    # step 5: verify the data using DBViewer 
    
    # step 6: define the dependencies 
    # extract 
    create_table() >> extract_apod  ## snsure the table is create before extraction 
    api_response=extract_apod.output
    # transform
    transformed_data=transform_apod_date(api_response)
    # load 
    load_data_postgres(transformed_data)


   