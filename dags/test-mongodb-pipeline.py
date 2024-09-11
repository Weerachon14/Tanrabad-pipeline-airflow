from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient
from datetime import datetime

# ฟังก์ชันในการเชื่อมต่อและดึงข้อมูลจาก MongoDB
def extract_from_mongo():
# ตั้งค่า SSH Tunnel
    with SSHTunnelForwarder(
        ('rtn.bigstream.cloud', 22),  # SSH Host และ Port
        ssh_username='trainee',  # SSH Username
        ssh_password='P@ssw0rd',  # SSH Password
        remote_bind_address=('localhost', 27017)  # เชื่อมต่อกับ MongoDB ที่อยู่บน localhost:27017 ของ server
    ) as tunnel:
        # เชื่อมต่อ MongoDB ผ่าน Tunnel
        client = MongoClient('mongodb://root:vkiNfu%3Auigrid@localhost:{}'.format(tunnel.local_bind_port))
        db = client['igrid']
        collection = db['twdata_dengue']

        # ดึงข้อมูล
        documents = collection.find({})
        for document in documents:
            print(document)

# สร้าง DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 11),
    'retries': 1,
}

with DAG(dag_id='test_mongo', 
         default_args=default_args,
         description='Start query mongoDB',
         schedule_interval=None, 
         catchup=False
) as dag:
    task1 = PythonOperator(
        task_id='extract_from_mongo',
        python_callable=extract_from_mongo
    )
