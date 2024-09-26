# Tanrabad-pipeline-airflow

## Overview
This project implements an ETL pipeline using **Apache Airflow**. The pipeline extracts data from a **MongoDB** database, processes the data by filtering and transforming it, and then saves the transformed results into JSON files for further use. The pipeline also computes various sentiment and dengue-related scores and details, which are stored alongside the original data.

## Project Structure

- DAG: The pipeline is managed by Apache Airflow DAGs and runs on a daily schedule.
- Tasks:
  1. Query MongoDB: Connects to a remote MongoDB instance via SSH tunnel, retrieves data from a specified collection, and saves the raw data into a JSON file.
  2. Filter Data: Filters the MongoDB data to retain only the required fields, then saves this filtered data to a new JSON file.
  3. Transform Labels: Analyzes the filtered data to calculate scores and labels for both `dengue` and `sentiment`. Additional detailed scores are computed and stored for each class in the labels.

## Pipeline Tasks Breakdown

### 1. Query MongoDB
- Establishes an **SSH tunnel** to connect securely to a remote MongoDB database.
- Extracts all records from the specified MongoDB collection (`twdata_dengue`).
- Saves the extracted data to a local JSON file: `/opt/airflow/data/mongo_data.json`.

### 2. Filter Data
- Processes the raw MongoDB data, selecting only the relevant fields (`text`, `text_cleaned`, `text_tokenized`, and `label`).
- Saves the filtered data to a new JSON file: `/opt/airflow/data/filtered_data.json`.

### 3. Transform Labels
- Computes the `dengue` and `sentiment` labels based on the content of the `label` field in the MongoDB records.
- Calculates the following for both `dengue` and `sentiment`:
  - Label (l)**: Represents whether the majority class in the label data is positive (1), negative (-1), or neutral (0).
  - Score: A ratio representing the occurrence of the majority class compared to the total number of labels.
  - Detail: An array storing the score for each possible class (`1`, `0`, `-1`) for `sentiment`, and the scores for `1` and `0` for `dengue`.
  
- Saves the transformed data with computed labels, scores, and details into a final JSON file:`.
