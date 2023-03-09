import pandas as pd
import json
import os
import requests
import logging
from datetime import datetime
from sqlalchemy import create_engine
from jinja2 import Template

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.email import EmailOperator


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

def find_high_volume_sales(**kwargs):
    '''Finds products in the DB that have sales more or equal than ARS$7.000.000, and sends a sample of them'''
    # We set up the connection to the database to get the information
    logging.info("Finding high volume sales...")
    pg_user, pg_pw, pg_host, pg_db = os.getenv("PG_USER"), os.getenv("PG_PW"), os.getenv("PG_HOST"), os.getenv("PG_DB")
    pg_engine = create_engine(f"postgresql://{pg_user}:{pg_pw}@{pg_host}/{pg_db}")
    pg_connection = pg_engine.connect()

    query_result = pg_connection.execute("SELECT id, site_id, title, price, sold_quantity, thumbnail, TO_CHAR(created_date, 'dd-mm-yyyy') FROM public.products WHERE price * sold_quantity >= 7000000")
    product_list = [{
        "id": r[0], 
        "site_id": r[1], 
        "title": r[2], 
        "price": r[3], 
        "sold_quantity": r[4], 
        "thumbnail": r[5], 
        "created_date": r[6]} for r in query_result]
    if product_list == []: # If there are no products, the email won't be send
        return None
    else:
        return json.dumps({"data": product_list})

def compose_email(**kwargs):
    '''Renders email template'''
    # We pull the data returned by the previous task using XComs
    ti = kwargs["ti"]
    data_dict = json.loads(ti.xcom_pull(task_ids="find_high_volume_sales"))
    product_list = data_dict["data"]
    # Then we define a Jinja template for the email, we render it and return it
    template = Template('''
        <!DOCTYPE html>
        <html>
        <body>

        <table>
            <tr>
                <th><bold>id</bold></th>
                <th><bold>site_id</bold></th>
                <th><bold>title</bold></th>
                <th><bold>price</bold></th>
                <th><bold>sold_quantity</bold></th>
                <th><bold>thumbnail</bold></th>
                <th><bold>created_date</bold></th>
            </tr>
            {% for product in products %}
            <tr>
                <td>{{ product.id }}</td>
                <td>{{ product.site_id }}</td>
                <td>{{ product.title }}</td>
                <td>{{ product.price }}</td>
                <td>{{ product.sold_quantity }}</td>
                <td>{{ product.thumbnail }}</td>
                <td>{{ product.created_date }}</td>
            </tr>
            {% endfor %}
        </table>

        </body>
        </html>
    ''')
    rendered_template = template.render(products=product_list)

    return rendered_template

def should_email_be_sent(**kwargs):
    '''ShortCircuit function that decides if the email is sent or not'''
    ti = kwargs["ti"]
    prev = ti.xcom_pull(task_ids="find_high_volume_sales")
    if prev == None:
        return False
    else:
        return True

# Block II: Definition of Airflow DAG and DB operations:
with DAG(
    dag_id="mercadolibre_pipeline",
    default_args={
        "owner": "Mauricio Barbieri",
        "retries": 0, # Because I needed to debug the DAG, I wanted it to fail immediately if something was wrong
        "provide_context": True}, # Context is necessary for the use of XComs
    schedule_interval="0 12 * * *", # We could have used @daily here, but I think 9:00 am (12:00 pm UTC) is better for this operation
    start_date=datetime(2023, 2, 27)
) as dag:
    
    etl_step = PythonOperator(
        task_id="etl_step",
        python_callable=etl_step
    )

    find_high_volume_sales = PythonOperator(
        task_id="find_high_volume_sales",
        python_callable=find_high_volume_sales
    )

    should_email_be_sent = ShortCircuitOperator(
        task_id="should_email_be_sent",
        python_callable=should_email_be_sent
    )

    compose_email = PythonOperator(
        task_id="compose_email",
        python_callable=compose_email
    )

    send_email = EmailOperator(
        task_id="send_email",
        conn_id="sendgrid_default",
        to="mbarbierif.ar@gmail.com",
        subject="Daily High Volume Sales Products",
        html_content="{{ ti.xcom_pull(task_ids='compose_email') }}"
    )

    etl_step >> find_high_volume_sales >> should_email_be_sent >> compose_email >> send_email
