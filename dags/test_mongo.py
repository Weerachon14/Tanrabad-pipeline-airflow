import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
from sshtunnel import SSHTunnelForwarder
from datetime import datetime

def test_mongo_connection(**kwargs):
    # ดึงค่า environment variables สำหรับ SSH และ MongoDB
    ssh_username = os.getenv('SSH_USERNAME')
    ssh_password = os.getenv('SSH_PASSWORD')
    mongo_username = os.getenv('MONGO_USERNAME')
    mongo_password = os.getenv('MONGO_PASSWORD')

    # แสดงค่า username และ password เพื่อตรวจสอบ (ไม่แนะนำใน production ให้ลบออกในกรณีนี้)
    print(f"SSH Username: {ssh_username}")
    print(f"MongoDB Username: {mongo_username}")
    print(f"MongoDB Password: {mongo_password}")

    # ตั้งค่า SSH Tunnel เพื่อเชื่อมต่อกับ MongoDB
    with SSHTunnelForwarder(
        ('rtn.bigstream.cloud', 22),  # SSH Host และ Port
        ssh_username=ssh_username,
        ssh_password=ssh_password,
        remote_bind_address=('localhost', 27017)  # ที่อยู่ของ MongoDB ภายในเซิร์ฟเวอร์
    ) as tunnel:
        # ตั้งค่า MongoDB client ด้วย username และ password
        client = MongoClient(f'mongodb://{mongo_username}:{mongo_password}@localhost:{tunnel.local_bind_port}')
        
        # ดึงข้อมูลจากฐานข้อมูล
        db = client['igrid']  # ชื่อ database ที่ต้องการใช้
        collection = db['twdata_dengue']  # ชื่อ collection ที่ต้องการใช้
        documents = list(collection.find().limit(5))  # ดึงเอกสารตัวอย่าง 5 รายการ
        
        # พิมพ์ผลลัพธ์เพื่อตรวจสอบ
        # print("Documents from MongoDB:", documents)

        # ส่งออกไฟล์ JSON เพื่อตรวจสอบการเชื่อมต่อ
        file_path = '/opt/airflow/data/mongo_test_data.json'
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(documents, file, ensure_ascii=False, indent=4)

        return file_path


# กำหนดค่า default_args สำหรับ DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 24),
    'retries': 1,
}

# สร้าง DAG สำหรับทดสอบการเชื่อมต่อ MongoDB
with DAG(
    dag_id='test_mongo_connection_dag',
    default_args=default_args,
    schedule_interval=None,  # รันแบบ manual
    catchup=False
) as dag:
    
    test_mongo_task = PythonOperator(
        task_id='test_mongo_connection',
        python_callable=test_mongo_connection
    )

test_mongo_task
