# Integration Strategy

![image](https://github.com/user-attachments/assets/5fa92044-9d5a-4d62-b1f8-c0b9bab200ae)

**Description:** this data warehouse was designed follow `Inmon approach` that integrated all of data into a single warehouse and it created several data marts associating sectors in government system
- **Data Source:** Multi-databases from different systems in governmental sector
- **Medallion Architecture:** Refining data across layers that has the goal of improving the structure and quality of data for better insights and analysis - `bronze -> silver -> gold`
- **Staging Area:** Ensuring independence between source database and data warehouse when performing transformations and aggergrates

# Data Pipline Automation

All of the step in this project was design to a data pipeline which can be automated to load raw data from source that then go in medallion procedure for ensuring the quality of information. Finally, it was passed into warehouse and data marts.
- **Scheduler:** leveraging Apache Airflow to automate end-to-end integration process
- **Environment:** this process was deployed on Docker containers including *Database Server* and *Airflow*
- **Transformation:** using Apache Spark engine which was Pyspark package in Python to process and aggregate information
