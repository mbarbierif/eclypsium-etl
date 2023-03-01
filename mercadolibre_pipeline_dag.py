import pandas as pd
import json
import os
import requests
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator


# Block I: Definition of Python functions and callables
def extract_list_of_categories() -> list:
    '''Scrapes site categories URL and generates a list of them'''
    CATEGORIES_URL = "https://api.mercadolibre.com/sites/MLA/categories"
    json_response = requests.get(CATEGORIES_URL) # Gets raw JSON response from URL
    dict_response = json.loads(json_response.text) # Loads the JSON data into a Python dictionary
    categories = [entry["id"] for entry in dict_response] # Builds the list from the dictionary

    return categories

def extract_category(category_id: str) -> dict:
    '''Extracts product results from a single category ID and returns a dictionary with it'''
    CATEGORY_SEARCH_URL = "https://api.mercadolibre.com/sites/MLA/search?category={}#json"
    json_response = requests.get(CATEGORY_SEARCH_URL.format(category_id)) # Gets raw JSON response from category URL
    dict_response = json.loads(json_response.text) # Loads JSON data into a Python dictionary

    return dict_response["results"] # Returns only the results

def extract_and_transform_step():
    '''First step of the DAG, extracts and processes the data to generate a CSV output'''
    if not os.path.exists("/home/airflow/gcs/dags/sql"):
        os.mkdir("/home/airflow/gcs/dags/sql")

    if os.path.exists("/home/airflow/gcs/dags/sql/daily_products.sql"):
        return 0
    else:
        # First we define a master PRODUCTS list
        PRODUCTS = []
        
        # Now we build the categories list so we can iterate through it and add information to the PRODUCTS list:
        categories = extract_list_of_categories()
        for category_id in categories:
            PRODUCTS += extract_category(category_id)

        # Now we focus on the required fields in order to build a DataFrame that we can export as SQL
        N = len(PRODUCTS) # This number will be used many times below
        df = pd.DataFrame({
            "id": [PRODUCTS[i]["id"] for i in range(N)],
            "site_id": [PRODUCTS[i]["site_id"] for i in range(N)],
            "title": [PRODUCTS[i]["title"] for i in range(N)],
            "price": [PRODUCTS[i]["price"] for i in range(N)],
            "sold_quantity": [PRODUCTS[i]["sold_quantity"] for i in range(N)],
            "thumbnail": [PRODUCTS[i]["thumbnail"] for i in range(N)],
            "created_date": datetime.now()
            }
        )

        with open("/home/airflow/gcs/dags/sql/daily_products.sql", "w") as file:
            file.write("")
        
        with open("/home/airflow/gcs/dags/sql/daily_products.sql", "a") as file:
            for i, r in df.iterrows():
                line = f"INSERT INTO products VALUES ('{r.id}','{r.site_id}','{r.title}','{r.price}','{r.sold_quantity}','{r.thumbnail}','{r.created_date}')\n"
                file.write(line)

# Block II: Definition of Airflow DAG and DB operations:
with DAG(
    dag_id="mercadolibre_pipeline",
    default_args={"owner":"Mauricio Barbieri"},
    schedule_interval="0 9 * * *",
    start_date=datetime(2023, 2, 27)
) as dag:
    
    extract_and_transform_step = PythonOperator(
        task_id="extract_and_transform_step",
        python_callable=extract_and_transform_step,
        retries=0
    )

    create_table_if = PostgresOperator(
        task_id="create_table_if",
        postgres_conn_id="postgres_wh",
        sql='''
            CREATE TABLE IF NOT EXISTS products (
            id VARCHAR NOT NULL,
            site_id VARCHAR NOT NULL,
            title VARCHAR NOT NULL,
            price INT NOT NULL,
            sold_quantity INT NOT NULL,
            thumbnail VARCHAR NOT NULL,
            created_date DATE NOT NULL);
            ''',
        retries=0
    )

    load_step = PostgresOperator(
        task_id="load_step",
        postgres_conn_id="postgres_wh",
        sql="/home/airflow/gcs/dags/sql/daily_products.sql",
        retries=0
    )

    extract_and_transform_step >> create_table_if >> load_step
