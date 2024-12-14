from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import asyncio
import json
import os
# import logging

from aeon_link_scraper import scrape_initial_links, scrape_new_links
from aeon_articles_scraper import scrape_articles
from database_operations import load_data, is_database_empty

# logging.basicConfig(
#     level=print, 
#     format='%(asctime)s - %(levelname)s: %(message)s',
#     datefmt='%Y-%m-%d %H:%M:%S'
# )
# logger = logging.getLogger(__name__)

def extract_links():

    try:
        is_empty, _ = is_database_empty()

        if is_empty == None:
            return
        
        if is_empty:
            print("Database is empty. Scraping all links")
            scrape_initial_links()
        else:
            print("Database is not empty. Scraping new links")
            scrape_new_links()

    except Exception as e:
        print(f"Unexpected error occured: {e}")

def extract_articles():

    try:
        asyncio.run(scrape_articles())
    except Exception as e:
        print(f"Unexpected error occured: {e}")


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1
}

with DAG(
    'aeon_scraper_dag',
    default_args=default_args,
    schedule_interval='@weekly', 
    catchup=False
) as dag:
    
    extract_links_task = PythonOperator(
        task_id='extract_links',
        python_callable=extract_links,
        dag=dag
    )
    
    extract_articles_task = PythonOperator(
        task_id='scrape_articles',
        python_callable=extract_articles,
        dag=dag
    )
    
    load_task = PythonOperator(
        task_id='load_to_database',
        python_callable=load_data,
        dag=dag
    )

    extract_links_task >> extract_articles_task >> load_task