
# Project Overview

this project involves creating ETL pipeline using Apache Airflow, the pipeline extracts data from an external API, transforms the data and loads into into Postgres database. The entire process is done using airflow to schedule, monitor and manage workflows 
Airflow hooks and operators to handle ETL 
The project leverages Docker to run Airflow and Postgres as services, ensuring an isolated and reproducible environment.


##  Features:
- **Market data API**: 
    - API :- https://www.alphavantage.co/  
- **Database**:
    - Postgres
- **Airflow**:
    - scheduling the ETL pipeline

- **UI**: 
    - Streamlit app to play with data 

- **Project Workflow**:
    - scheduled data extraction using Airflow
    - tranforming the data and pusing it to postgres 
    - using streamlit to view the data 


## Apache Airflow 

### ETL pipeline
![image1](https://github.com/Immortal-Pi/ETL_pipeline_Airflow/blob/main/screenshots/1.png)
### logs 
![image2](https://github.com/Immortal-Pi/ETL_pipeline_Airflow/blob/main/screenshots/2.png)
### extracted data 
![image3](https://github.com/Immortal-Pi/ETL_pipeline_Airflow/blob/main/screenshots/3.png)
## DB Viewer 
![image8](https://github.com/Immortal-Pi/ETL_pipeline_Airflow/blob/main/screenshots/8.png)
## Streamlit
![image4](https://github.com/Immortal-Pi/ETL_pipeline_Airflow/blob/main/screenshots/4.png)

![image5](https://github.com/Immortal-Pi/ETL_pipeline_Airflow/blob/main/screenshots/5.png)

## Dockers
![image6](https://github.com/Immortal-Pi/ETL_pipeline_Airflow/blob/main/screenshots/6.png)
![image7](https://github.com/Immortal-Pi/ETL_pipeline_Airflow/blob/main/screenshots/7.png)

## Tech Stack 

- **Programming Language**: Python
- **MLOps Tools**:
    - Docker for containerization
    - Apache Airflow for ETL pipeline 
- **Web Framework**: streamlit for frontend
- **Database**: postgres for storing the data from API

## Conclusion
This project involves creating an ETL pipeline using Apache Airflow, Docker, and PostgreSQL. The pipeline extracts trading data from alpha vantage using API, transforms it, and loads it into a PostgreSQL database. Here's a summary of the workflow:

Extract (E): Data is fetched from Alpha Vantage API using Airflow's SimpleHttpOperator, returning JSON data containing fields like the title, explanation, and price data.

Transform (T): The extracted JSON data is processed in a transformation task using Airflow’s TaskFlow API, ensuring the data is in the correct format for the database (e.g., extracting relevant fields like title, close price, market name, date etc).

Load (L): The transformed data is loaded into a PostgreSQL database using Airflow's PostgresHook. If the target table doesn't exist, it's created automatically within the DAG.

The pipeline is orchestrated using an Airflow DAG, which manages task dependencies and ensures the process runs sequentially. Docker is used to run both Airflow and PostgreSQL in isolated containers, ensuring a reproducible environment with data persistence.
