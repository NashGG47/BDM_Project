from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import logging
import os

def run_environmental_indicators_cleaning():
    """Clean environmental indicators data and move to trusted zone."""
    try:
        result = subprocess.run(
            ["python", "trusted_zone/clean_environmental_indicators.py"],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info("Environmental indicators cleaning completed:\n%s", result.stdout)
        return "Environmental indicators cleaning successful"
    except subprocess.CalledProcessError as e:
        logging.error("Error cleaning environmental indicators: %s", e.stderr)
        raise

def run_passenger_volume_cleaning():
    """Clean passenger volume data and move to trusted zone."""
    try:
        result = subprocess.run(
            ["python", "trusted_zone/clean_passenger_volume.py"],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info("Passenger volume cleaning completed:\n%s", result.stdout)
        return "Passenger volume cleaning successful"
    except subprocess.CalledProcessError as e:
        logging.error("Error cleaning passenger volume: %s", e.stderr)
        raise

def run_admin_shapefiles_cleaning():
    """Clean administrative shapefiles data and move to trusted zone."""
    try:
        result = subprocess.run(
            ["python", "trusted_zone/clean_admin_shapefiles.py"],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info("Admin shapefiles cleaning completed:\n%s", result.stdout)
        return "Admin shapefiles cleaning successful"
    except subprocess.CalledProcessError as e:
        logging.error("Error cleaning admin shapefiles: %s", e.stderr)
        raise

def validate_trusted_data():
    """Simple validation that trusted zone data was created properly."""
    try:
        import duckdb
        con = duckdb.connect("bdm_project.duckdb")
        
        # Check for expected trusted tables
        expected_tables = [
            'trusted_environmental_indicators',
            'trusted_passenger_volume', 
            'trusted_admin_boundaries'
        ]
        
        for table in expected_tables:
            try:
                count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                logging.info(f"Table {table}: {count} rows")
                if count == 0:
                    logging.warning(f"Table {table} is empty!")
            except Exception as e:
                logging.error(f"Table {table} validation failed: {e}")
                raise
        
        con.close()
        logging.info("All trusted zone tables validated successfully")
        return "Data validation successful"
        
    except Exception as e:
        logging.error(f"Data validation failed: {e}")
        raise

def cleanup_old_files():
    """Clean up old trusted zone files to save space."""
    try:
        import os
        from pathlib import Path
        from datetime import datetime, timedelta
        
        # Define directories to clean
        trusted_dirs = [
            "storage/delta/trusted/structured/environmental_indicators/",
            "storage/delta/trusted/structured/passenger_volume/",
            "storage/delta/trusted/structured/administrative_shapefiles/"
        ]
        
        cutoff_date = datetime.now() - timedelta(days=30)  # Keep last 30 days
        files_removed = 0
        
        for dir_path in trusted_dirs:
            if os.path.exists(dir_path):
                for file_path in Path(dir_path).glob("*.parquet"):
                    file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if file_mtime < cutoff_date:
                        file_path.unlink()
                        files_removed += 1
                        
        logging.info(f"Cleanup completed: {files_removed} old files removed")
        return f"Cleanup successful: {files_removed} files removed"
        
    except Exception as e:
        logging.error(f"Cleanup failed: {e}")
        return "Cleanup completed with warnings"

# DAG configuration
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 15),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'trusted_zone_structured_data',
    default_args=default_args,
    description='Process and clean structured data for trusted zone',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    tags=["trusted_zone", "structured_data"],
)

# Data cleaning tasks
clean_environmental = PythonOperator(
    task_id='clean_environmental_indicators',
    python_callable=run_environmental_indicators_cleaning,
    dag=dag,
)

clean_passenger = PythonOperator(
    task_id='clean_passenger_volume',
    python_callable=run_passenger_volume_cleaning,
    dag=dag,
)

clean_admin = PythonOperator(
    task_id='clean_admin_shapefiles',
    python_callable=run_admin_shapefiles_cleaning,
    dag=dag,
)

# Validation task
validate = PythonOperator(
    task_id='validate_trusted_data',
    python_callable=validate_trusted_data,
    dag=dag,
)

cleanup = PythonOperator(
    task_id='cleanup_old_files',
    python_callable=cleanup_old_files,
    dag=dag,
)

# Task dependencies - all cleaning tasks run in parallel, then validation, then cleanup
[clean_environmental, clean_passenger, clean_admin] >> validate >> cleanup