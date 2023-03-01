import pandas as pd
import json
import os
import requests
import logging
from datetime import datetime
from sqlalchemy import create_engine

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

def etl_step():
    '''First step of the DAG, extracts, processes and loads the data to a PostgreSQL instance in Cloud SQL'''
    # First we define a master PRODUCTS list
    PRODUCTS = []
    
    # Now we build the categories list so we can iterate through it and add information to the PRODUCTS list:
    logging.info("Extracting categories data...")
    categories = extract_list_of_categories()
    logging.info("Extracting products from each category...")
    for category_id in categories:
        PRODUCTS += extract_category(category_id)

    # Now we focus on the required fields in order to build a DataFrame that we can export as SQL
    logging.info("Processing product data and building DataFrame...")
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

    # We set up the connection to the database and load the DataFrame
    logging.info("Setting up connection and loading data...")
    pg_user, pg_pw, pg_host, pg_db = os.getenv("PG_USER"), os.getenv("PG_PW"), os.getenv("PG_HOST"), os.getenv("PG_DB")
    pg_engine = create_engine(f"postgresql://{pg_user}:{pg_pw}@{pg_host}/{pg_db}")
    df.to_sql(name="products", con=pg_engine, if_exists="replace")
        

# Block II: Definition of Airflow DAG and DB operations:
with DAG(
    dag_id="mercadolibre_pipeline",
    default_args={"owner":"Mauricio Barbieri"},
    schedule_interval="0 9 * * *",
    start_date=datetime(2023, 2, 27)
) as dag:
    
    etl_step = PythonOperator(
        task_id="etl_step",
        python_callable=etl_step,
        retries=0
    )

    etl_step
