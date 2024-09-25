import json
import os
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient
from airflow.models import Variable
# from urllib.parse import quote_plus


# task 1
def query_data_from_mongo(**kwargs):
    # ดึงค่า environment variables สำหรับ SSH และ MongoDB
    mongo_username = Variable.get("MONGO_USERNAME")
    mongo_password = Variable.get("MONGO_PASSWORD")
    ssh_username = Variable.get("SSH_USERNAME")
    ssh_password = Variable.get("SSH_PASSWORD")
    
    # แสดงค่า username และ password เพื่อตรวจสอบ (ไม่แนะนำใน production ให้ลบออกในกรณีนี้)
    print(f"SSH Username: {ssh_username}")
    print(f"MongoDB User:Pass: {mongo_username}:{mongo_password}")

    # ตั้งค่า SSH Tunnel เพื่อเชื่อมต่อกับ MongoDB
    with SSHTunnelForwarder(
        ('rtn.bigstream.cloud', 22),  # SSH Host และ Port
        ssh_username=ssh_username,
        ssh_password=ssh_password,
        remote_bind_address=('localhost', 27017)  # ที่อยู่ของ MongoDB ภายในเซิร์ฟเวอร์
    ) as tunnel:
        # ตั้งค่า MongoDB client ด้วย username และ password
        client = MongoClient(f'mongodb://{mongo_username}:{mongo_password}@localhost:{tunnel.local_bind_port}')
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
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(documents, file, ensure_ascii=False, indent=4)

        # ส่งพาธไฟล์ไปยัง XCom เพื่อใช้ใน task ต่อไป
        kwargs['ti'].xcom_push(key='mongo_file_path', value=file_path)

        return file_path  # ส่งพาธไฟล์กลับไปยัง XCom

#task 2
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
    print("Filter data: ",filtered_data)
    
    # กำหนดพาธไฟล์ใหม่
    filtered_file_path = '/opt/airflow/data/filtered_data.json'
    
    # ตรวจสอบและสร้างไดเรกทอรีหากยังไม่มี
    directory = os.path.dirname(filtered_file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    # บันทึกข้อมูลที่ตัดแล้วเป็นไฟล์ JSON ใหม่
    with open(filtered_file_path, 'w', encoding='utf-8') as filtered_file:
        json.dump(filtered_data, filtered_file, ensure_ascii=False, indent = 4)
    
    # ส่งพาธไฟล์ไปยัง XCom เพื่อใช้ใน task อื่น
    ti.xcom_push(key='filtered_file_path', value=filtered_file_path)

    return filtered_file_path


# def transform_label(**kwargs):
#     ti = kwargs['ti']
#     file_path = ti.xcom_pull(key='filtered_file_path', task_ids='filter_data')
    
#     if not file_path:
#         raise ValueError("file_path is None. Make sure the previous task has pushed the correct flie to XCom")

#     with open(file_path, 'r') as file:
#         data = json.load(file)
      
#     transformed_data = []
#     for item in data:
#         label_data = item.get('label', [])
#         # ตรวจสอบว่า label_data เป็น list หรือไม่
#         if label_data is None:
#             label_data = []
        
#         # Transform L in label
#         for label in label_data:
#             if 'l' in label:
#                 if label['l'].startswith('Y'):
#                     label['l'] = 1
#                 elif label['l'].startswith('N'):
#                     label['l'] = 0
    
#         transformed_field = {
#             'text': item.get('text'),
#             'text_cleaned': item.get('text_cleaned'),
#             'text_tokenized': item.get('text_tokenized'),
#             'label': label_data
#         }
#         transformed_data.append(transformed_field)
    
#     print("Transform")
    
    # transformed_label_file_path = '/opt/airflow/data/transformed_label_data.json'    
    
    # directory = os.path.dirname(transformed_label_file_path) 
    # if not os.path.exists(directory):
    #     os.makedirs(directory)
        
    # with open(transformed_label_file_path, 'w', encoding='utf-8') as transformed_label_file:
    #     json.dump(transformed_data, transformed_label_file, ensure_ascii=False, indent=4)
                        
    # ti.xcom_push(key='transformed_label_file_path', value= transformed_label_file_path)
    # return transformed_label_file_path


#task 3 
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
        
        # คำนวณจำนวน total_labels และจำนวนที่ค่า 'l' ใน dengue เป็น 1
        total_labels = len(label_data)
        count_l_dengue = sum(1 for label in label_data if label.get('l', '').startswith('Y'))
        
        # คำนวณค่า score โดยใช้ค่าเฉลี่ยของ 'l' ใน dengue
        score_dengue = count_l_dengue / total_labels if total_labels > 0 else 0
            
        # ค่า 'l' ใน dengue จะเป็น 1 ถ้าหากค่า count_l_dengue มากกว่าหรือเท่ากับครึ่งหนึ่งของ total_labels
        dengue_l = 1 if count_l_dengue >= total_labels / 2 else 0

        # สำหรับ sentiment
        sentiment_l_values = []
        for label in label_data:
            label_l = label.get('l', '')
            if label_l == 'YBP':
                sentiment_l_values.append(1)
            elif label_l == 'YBO':
                sentiment_l_values.append(0)
            elif label_l == 'YBN':
                sentiment_l_values.append(-1)

        # คำนวณค่า 'l' ของ sentiment โดยหาค่าที่ปรากฏบ่อยที่สุด
        if sentiment_l_values:
            count_l_sentiment_1 = sentiment_l_values.count(1)
            count_l_sentiment_0 = sentiment_l_values.count(0)
            count_l_sentiment_minus_1 = sentiment_l_values.count(-1)

            # หา 'l' ที่ปรากฏมากที่สุดใน sentiment
            if count_l_sentiment_1 >= total_labels / 2:
                sentiment_l = 1
            elif count_l_sentiment_0 >= total_labels / 2:
                sentiment_l = 0
            elif count_l_sentiment_minus_1 >= total_labels / 2:
                sentiment_l = -1
            else:
                sentiment_l = 0  # ค่าดีฟอลต์ถ้าไม่มีเงื่อนไขเข้า

            # คำนวณค่า max สำหรับใช้เป็น base ในการหา score สำหรับ sentiment
            sentiment_score = max(count_l_sentiment_1, count_l_sentiment_0, count_l_sentiment_minus_1) / total_labels if total_labels > 0 else 0
        else:
            sentiment_l = 0
            sentiment_score = 0
        
        # เก็บข้อมูล transformed data
        transformed_field = {
            'text': item.get('text'),
            'text_cleaned': item.get('text_cleaned'),
            'text_tokenized': item.get('text_tokenized'),
            'label': {
                'dengue': {
                    'l': dengue_l,
                    'score': round(score_dengue, 2)  # ปัดค่า score ให้เป็นทศนิยม 2 ตำแหน่ง
                },
                'sentiment': {
                    'l': sentiment_l,
                    'score': round(sentiment_score, 2)  # ปัดค่า score ให้เป็นทศนิยม 2 ตำแหน่ง
                }
            }
        }
        transformed_data.append(transformed_field)
    print("transformed_data: ",transformed_data)
        
    transformed_label_to_dengue_file_path = '/opt/airflow/data/transformed_label_data.json'
    print(f"Saving file to: {transformed_label_to_dengue_file_path}")

    directory = os.path.dirname(transformed_label_to_dengue_file_path) 
    if not os.path.exists(directory):
        os.makedirs(directory)
        
    with open(transformed_label_to_dengue_file_path, 'w', encoding='utf-8') as transformed_label_file:
        json.dump(transformed_data, transformed_label_file, ensure_ascii=False, indent=4)
                        
    ti.xcom_push(key='transform_label_to_dengue', value=transformed_label_to_dengue_file_path)
    return transformed_label_to_dengue_file_path


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 18),
    'retries': 1
}

with DAG(dag_id = 'dengue_pipeline',
         default_args = default_args,
         description = 'Start pipeline by query and save file',
         schedule='@daily',
         catchup = False
) as dag:
    # Task 1
    Query_mongo = PythonOperator(
        task_id = 'query_data_from_mongo',
        python_callable = query_data_from_mongo
    )
    # Task 2
    Filter_task = PythonOperator(
        task_id = 'filter_data',
        python_callable = filter_data
    )
    # Task 3 
    Transform_label_to_dengue = PythonOperator(
        task_id = "transform_label_to_dengue",
        python_callable = transform_label_to_dengue,
        trigger_rule="all_done"
    )

Query_mongo >> Filter_task >>  Transform_label_to_dengue 