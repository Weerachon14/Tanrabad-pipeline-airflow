FROM apache/airflow:2.10.0

# Switch to root to install dependencies
USER root

# Copy requirements.txt to the container
COPY requirements.txt /requirements.txt

# Change ownership of requirements.txt to airflow user
RUN chown airflow /requirements.txt

# Switch to the airflow user to install the dependencies
USER airflow

# Install dependencies globally without --user flag
RUN pip install -r /requirements.txt

# Set the default command to start the Airflow webserver
CMD ["webserver"]