import json
import os
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient

#Function task 1
 
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


def check_json_file_task2(**kwargs):
    # ดึง path ของไฟล์จาก XCom
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='mongo_file_path', task_ids='query_data_from_mongo')

    # ตรวจสอบว่า file_path ได้ถูกดึงมาจาก XCom หรือไม่
    if not file_path:
        raise ValueError("file_path is None. Make sure the previous task has pushed the correct file path to XCom.")

    # อ่านข้อมูลจากไฟล์ JSON
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            print("JSON data:", data)  # พิมพ์ข้อมูล JSON เพื่อตรวจสอบ
    except Exception as e:
        raise ValueError(f"Error reading JSON file: {e}")


 
#Function task 2

def filter_data(**kwargs):
    # ดึง path ของไฟล์จาก XCom
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='mongo_file_path', task_ids='query_data_from_mongo')

    # ตรวจสอบว่า file_path ได้ถูกดึงมาจาก XCom หรือไม่
    if not file_path:
        raise ValueError("file_path is None. Make sure the previous task has pushed the correct file path to XCom.")

    # อ่านข้อมูลจากไฟล์ JSON ที่ได้จาก task แรก
    with open(file_path, 'r') as file:
        data = json.load(file)
    
    # กำหนดคอลัมน์ที่ต้องการตัดข้อมูล
    filtered_data = []
    for item in data:
        filtered_item = {
            'text': item.get('text'),
            'text_cleaned': item.get('text_cleaned'),
            'text_tokenized': item.get('text_tokenized'),
            'label': item.get('label')
        }
        filtered_data.append(filtered_item)

    # กำหนดพาธไฟล์ใหม่
    filtered_file_path = '/opt/airflow/data/filtered_data.json'
    
    # ตรวจสอบและสร้างไดเรกทอรีหากยังไม่มี
    directory = os.path.dirname(filtered_file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    # บันทึกข้อมูลที่ตัดแล้วเป็นไฟล์ JSON ใหม่
    with open(filtered_file_path, 'w') as filtered_file:
        json.dump(filtered_data, filtered_file)
    
    # ส่งพาธไฟล์ไปยัง XCom เพื่อใช้ใน task อื่น
    ti.xcom_push(key='filtered_file_path', value=filtered_file_path)

    return filtered_file_path



def check_json_file_task3(**kwargs):
    # ดึง path ของไฟล์จาก XCom
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='transformed_label_file_path', task_ids='transform_label')

    # ตรวจสอบว่า file_path ได้ถูกดึงมาจาก XCom หรือไม่
    if not file_path:
        raise ValueError("file_path is None. Make sure the previous task has pushed the correct file path to XCom.")

    # อ่านข้อมูลจากไฟล์ JSON
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            print("JSON data:", data)  # พิมพ์ข้อมูล JSON เพื่อตรวจสอบ
    except Exception as e:
        raise ValueError(f"Error reading JSON file: {e}")

def transform_label(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='filtered_file_path', task_ids='filter_data')
    
    if not file_path:
        raise ValueError("file_path is None. Make sure the previous task has pushed the correct flie to XCom")

    with open(file_path, 'r') as file:
        data = json.load(file)
      
    transformed_data = []
    for item in data:
        label_data = item.get('label', [])
        # ตรวจสอบว่า label_data เป็น list หรือไม่
        if label_data is None:
            label_data = []
        
        # Transform L in label
        for label in label_data:
            if 'l' in label:
                if label['l'].startswith('Y'):
                    label['l'] = 1
                elif label['l'].startswith('N'):
                    label['l'] = 0
    
        transformed_field = {
            'text': item.get('text'),
            'text_cleaned': item.get('text_cleaned'),
            'text_tokenized': item.get('text_tokenized'),
            'label': label_data
        }
        transformed_data.append(transformed_field)
        
    transformed_label_file_path = '/opt/airflow/data/transformed_label_data.json'    
    
    directory = os.path.dirname(transformed_label_file_path) 
    if not os.path.exists(directory):
        os.makedirs(directory)
        
    with open(transformed_label_file_path, 'w') as transformed_label_file:
        json.dump(transformed_data, transformed_label_file)
                        
    ti.xcom_push(key='transformed_label_file_path', value= transformed_label_file_path)
    return transformed_label_file_path

def transform_label_to_dengue(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='filtered_file_path', task_ids='filter_data')
    
    if not file_path:
        raise ValueError("file_path is None. Make sure the previous task has pushed the correct file to XCom")

    with open(file_path, 'r') as file:
        data = json.load(file)
      
    transformed_data = []
    
    for item in data:
        label_data = item.get('label', [])
            
        # ตรวจสอบว่า label_data เป็น list หรือไม่
        if not label_data:
            label_data = []
        print(f"label_data: {label_data}")
        
        # คำนวณจำนวน total_labels และจำนวนที่ค่า 'l' เป็น 1
        total_labels = len(label_data)
        count_l = sum(1 for label in label_data if label.get('l', '').startswith('Y'))
        print(f"Total labels: {total_labels}, Count of 'l' = 1: {count_l}")
        
        # คำนวณค่า score โดยใช้ค่าเฉลี่ยของ 'l'
        score = count_l / total_labels if total_labels > 0 else 0
            
        # ค่า 'l' ใน dengue จะเป็น 1 ถ้าหากค่า count_l มากกว่าหรือเท่ากับครึ่งหนึ่งของ total_labels
        dengue_l = 1 if count_l >= total_labels / 2 else 0
            
        transformed_field = {
            'text': item.get('text'),
            'text_cleaned': item.get('text_cleaned'),
            'text_tokenized': item.get('text_tokenized'),
            'label': {
                'dengue': {
                    'l': dengue_l,
                    'score': round(score, 2)  # ปัดค่า score ให้เป็นทศนิยม 2 ตำแหน่ง
                }
            }
        }
        transformed_data.append(transformed_field)
        
    transformed_label_to_dengue_file_path = '/opt/airflow/data/transformed_label_data.json'
    
    directory = os.path.dirname(transformed_label_to_dengue_file_path) 
    if not os.path.exists(directory):
        os.makedirs(directory)
        
    with open(transformed_label_to_dengue_file_path, 'w') as transformed_label_file:
        json.dump(transformed_data, transformed_label_file)
                        
    ti.xcom_push(key='transform_label_to_dengue', value=transformed_label_to_dengue_file_path)
    return transformed_label_to_dengue_file_path



def check_json_file_task4(**kwargs):
    # ดึง path ของไฟล์จาก XCom
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='transform_label_to_dengue', task_ids='transform_label_to_dengue')

    # ตรวจสอบว่า file_path ได้ถูกดึงมาจาก XCom หรือไม่
    if not file_path:
        raise ValueError("file_path is None. Make sure the previous task has pushed the correct file path to XCom.")

    # อ่านข้อมูลจากไฟล์ JSON
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            print("JSON data:", data)  # พิมพ์ข้อมูล JSON เพื่อตรวจสอบ
    except Exception as e:
        raise ValueError(f"Error reading JSON file: {e}")



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 19),
    'retries': 1
}

with DAG(dag_id = 'dengue1',
         default_args = default_args,
         description = 'Start pipeline by query and save file',
         schedule_interval = None,
         catchup = False
) as dag:
    
    # Task 1
    Query_mongo = PythonOperator(
        task_id = 'query_data_from_mongo',
        python_callable = query_data_from_mongo
        # Task ที่ 2: ตัดคอลัมน์และบันทึกไฟล์ใหม่
    )
    # Task 2
    Filter_task = PythonOperator(
        task_id = 'filter_data',
        python_callable = filter_data,
        provide_context = True
    )
    # Check data in Task 2
    Check_json1 = PythonOperator(
        task_id='check_json_file2',
        python_callable=check_json_file_task2,
        trigger_rule='all_done'  # เพิ่ม trigger rule ตรงนี้
    )
    
    # Task 3 
    Transform_label_task = PythonOperator(
        task_id = 'transform_label',
        python_callable = transform_label,
        provide_context = True
    )
    
        # Check data in Task 3
    Check_json2 = PythonOperator(
        task_id='check_json_file3',
        python_callable=check_json_file_task3,
        trigger_rule='all_done'  # เพิ่ม trigger rule ตรงนี้
    )
    # Task 4 
    Transform_label_to_dengue = PythonOperator(
        task_id = "transform_label_to_dengue",
        python_callable = transform_label_to_dengue,
        trigger_rule="all_done"
    )
    Check_json3 = PythonOperator(
        task_id='check_json_file4',
        python_callable=check_json_file_task4,
        trigger_rule='all_done'  # เพิ่ม trigger rule ตรงนี้
    )
    

Query_mongo >> Filter_task >> Check_json1 >> Transform_label_task >> Check_json2 >> Transform_label_to_dengue >> Check_json3