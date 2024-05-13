'''
=================================================
Milestone 3

Nama  : Gede Aldi
Batch : FTDS-RMT-029

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL, membersihkan datanya dan menaruhnya ke ElasticSearch.
Data yang digunakan adalah data performa dan attrition karyawan
=================================================
'''

import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch



def queryPostgresql():
    ''' Fungsi ini digunakan untuk mengimport data dari PostgreSQL
    dan mendifine nya dalam bentuk df'''

    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow' port = '5432' "
    conn=db.connect(conn_string)
    
    df=pd.read_sql("select * from table_m3",conn)
    df.to_csv('/opt/airflow/dags/P2M3_GedeAldi_data_raw.csv')
    print("-------Data Saved------")
    conn.close()


def data_cleaning():
    ''' Fungsi ini digunakan untuk membersihkan dataframe dengan men drop duplikat, 
    mengubah huruf pada judul kolom menjadi lowercase, mengganti spasi dengan underscore,
    menghapus simbol dan juga handling missing value'''

    df=pd.read_csv('/opt/airflow/dags/P2M3_GedeAldi_data_raw.csv')
    df.drop_duplicates(inplace=True) # Handling Duplikat
    df.columns = df.columns.str.lower()  # Ubah semua huruf menjadi lowercase
    df.columns = df.columns.str.strip()  # Hapus spasi yang tidak diperlukan di awal atau akhir nama kolom
    df.columns = df.columns.str.replace(' ', '_')  # Ganti spasi dengan underscore
    df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '')  # Hapus simbol tidak diperlukan
    df.fillna(value=pd.np.nan, inplace=True) # Handling missing value
    df.to_csv('/opt/airflow/dags/P2M3_GedeAldi_data_clean.csv', index=False)


def insertElasticsearch():
    ''' Fungsi ini digunakan untuk menaruh data yang sudah bersih ke elasticsearch agar
    bisa dibuka di kibana'''
    es = Elasticsearch("http://elasticsearch:9200") 
    df=pd.read_csv('/opt/airflow/dags/P2M3_GedeAldi_data_clean.csv')
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="project_m3_fix",doc_type="doc",body=doc)
        print(res)


default_args = {
    'owner': 'Gede',
    'start_date': dt.datetime(2024, 4, 27, 22, 0, 0) - dt.timedelta(hours=8),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG('M3_DAG',
         default_args=default_args,
         schedule_interval='30 6 * * *',      
         ) as dag:

    getData = PythonOperator(task_id='QueryPostgreSQL',
                                 python_callable=queryPostgresql)
    
    cleanData = PythonOperator(task_id='clean_and_save_to_csv',
                                 python_callable=data_cleaning)
    
    insertData = PythonOperator(task_id='InsertDataElasticsearch',
                                 python_callable=insertElasticsearch)
    


getData >> cleanData >> insertData