import json
import os
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient

def query_data_from_mongo(**kwargs):
    # ตั้งค่า SSH Tunnel เพื่อ Access Mongo
    with SSHTunnelForwarder(
        ('rtn.bigstream.cloud', 22),  # SSH Host และ Port
        ssh_username='trainee',
        ssh_password='P@ssw0rd',
        remote_bind_address=('localhost', 27017)  # เชื่อมต่อกับ MongoDB ที่อยู่บน localhost:27017 ของ server
    ) as tunnel:
        client = MongoClient('mongodb://root:vkiNfu%3Auigrid@localhost:{}'.format(tunnel.local_bind_port))
        db = client['igrid']
        collection = db['twdata_dengue']
        
        # ดึงข้อมูลจาก MongoDB
        documents = list(collection.find({}))
        
        # กำหนดพาธไฟล์
        file_path = '/opt/airflow/data/mongo_data.json'  # พาธไฟล์
        
        # ตรวจสอบและสร้างไดเรกทอรีหากยังไม่มี
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        # บันทึกข้อมูลเป็นไฟล์ JSON
        with open(file_path, 'w') as file:
            json.dump(documents, file)
        
        # ส่งพาธไฟล์ไปยัง XCom เพื่อใช้ใน task ต่อไป
        kwargs['ti'].xcom_push(key='mongo_file_path', value=file_path)
        
        return file_path  # ส่งพาธไฟล์กลับไปยัง XCom

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 11),
    'retries': 1
}

with DAG(dag_id = 'Start_Pipeline01',
         default_args = default_args,
         description = 'Start pipeline by query and save file',
         schedule_interval = None,
         catchup = False
) as dag:
    Query_mongo = PythonOperator(
        task_id = 'query_data_from_mongo',
        python_callable = query_data_from_mongo
    )