# BDM_Project
Big Data Management  - Project

📂 BDM_Project  
│  
├── 📜 README.md                  # Project overview and setup instructions  
├── 📜 requirements.txt            # Python dependencies for the project  
├── 📜 LICENSE                     # Open-source license (if applicable)  
│  
├── 🛠 configs                      # Configuration files for Spark, Airflow, and Delta Lake  
│   ├── 📜 airflow-webserver.pid    # Airflow webserver process ID  
│   ├── 📜 airflow.cfg              # Airflow configuration file  
│   ├── 📜 airflow.db               # Airflow metadata database  
│   ├── 📜 delta-lake.yml           # Delta Lake configuration  
│   ├── 📂 logs                     # Airflow logs directory  
│   │   └── 📂 scheduler            # Airflow scheduler logs  
│   ├── 📜 spark-defaults.conf      # Default Spark configurations  
│   └── 📜 webserver_config.py      # Airflow webserver settings  
│  
├── 📂 data_ingestion               # Handles data ingestion from various sources  
│   ├── 📂 airflow                  # Airflow DAGs for data processing automation  
│   │   └── 📂 dags                 # Folder containing DAG scripts  
│   ├── 📂 hot_paths                # Real-time data ingestion pipeline  
│   │   ├── 📂 consumer             # Kafka consumer scripts  
│   │   └── 📂 producer             # Kafka producer scripts  
│   ├── 📂 warm_paths               # Near real-time ingestion (batch processing)  
│   └── 📂 cold_paths               # Scheduled batch data ingestion  
│  
├── 📂 data_processing              # Scripts for cleaning, transforming, and structuring data  
│   └── 📜 transform.py             # Transformation logic for ingested data  
│  
├── 📂 docs                         # Documentation and project description  
│   └── 📜 project_description.md   # Detailed project documentation  
│  
├── 📂 landing_zone                 # Temporary storage for raw ingested data  
│   └── 📜 process.py               # Processing raw data before transformation  
│  
├── 📂 notebooks                    # Jupyter notebooks for data exploration & testing  
│   └── 📜 data_exploration_test.ipynb  # Exploratory Data Analysis notebook  
│  
├── 📂 storage                      # Persistent storage for structured data  
│   └── 📂 delta                    # Delta Lake storage  
│       └── 📂 raw                  # Raw data stored in Delta format  