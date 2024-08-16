# Integration Strategy

![image](https://github.com/user-attachments/assets/5fa92044-9d5a-4d62-b1f8-c0b9bab200ae)

**Description:** this data warehouse was designed follow `Inmon approach` that integrated all of data into a single warehouse and it created several data marts associating sectors in government system
- **Data Source:** Multi-databases from different systems in governmental sector
- **Medallion Architecture:** Refining data across layers that has the goal of improving the structure and quality of data for better insights and analysis - `bronze -> silver -> gold`
- **Staging Area:** Ensuring independence between source database and data warehouse when performing transformations and aggergrates

# Data Pipline Automation

All of the step in this project was design to a data pipeline which can be automated to load raw data from source that then go in medallion procedure for ensuring the quality of information. Finally, it was passed into warehouse and data marts.
- **Scheduler:** leveraging Apache Airflow to automate end-to-end integration process
- **Transformation:** using Apache Spark engine which was Pyspark package in Python to process and aggregate information
- **Environment:** this process was deployed on Docker containers including *Database Server* and *Airflow*


### Docker setup

Dockerfile for Airflow and Spark
```dockerfile
FROM apache/airflow:2.9.1-python3.11

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

# Sync files from local to Docker image
COPY ./airflow/dags /opt/airflow/dags
COPY requirements.txt .

# Pyspark package
RUN pip install --no-cache-dir -r requirements.txt
RUN rm requirements.txt
```


DAGs of data warehouse integration

![image](https://github.com/user-attachments/assets/91cd725b-35f7-49f8-a173-f086a9024a22)



DAGs of Resident data mart integration

![image](https://github.com/user-attachments/assets/0fbc0c9e-b7b3-4492-bac7-89d4dbd3aa57)


