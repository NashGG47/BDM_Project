## How to Install and Run MinIO (Without Docker) in WSL

This project uses MinIO to store unstructured data (images, Parquet files, and metadata) in the Trusted Zone. Follow these steps to install and run MinIO locally in WSL without Docker.

### Step 1: Navigate to the root of the project  
cd ~/BDM_Project-unstructured_p2

### Step 2: Create the MinIO storage folder  
mkdir -p storage/minio/data

This folder will hold all data managed by MinIO (buckets, objects, metadata...).

### Step 3: Download the MinIO binary  
wget https://dl.min.io/server/minio/release/linux-amd64/minio  
chmod +x minio  
sudo mv minio /usr/local/bin/

This downloads the MinIO server binary, makes it executable, and moves it to a global path so you can use it anywhere.

### Step 4: Start MinIO with the correct folder  
cd ~/BDM_Project-unstructured_p2  
MINIO_ROOT_USER=admin MINIO_ROOT_PASSWORD=admin123 minio server storage/minio/data --console-address ":9001"

- API endpoint: http://localhost:9000 (used by your Python scripts)  
- Console: http://localhost:9001 (web UI)  
- Username: admin  
- Password: admin123

Leave this terminal open while MinIO is running.

### Step 5: Open the MinIO Console in your browser  
http://localhost:9001

Login with:  
Username: admin  
Password: admin123

### Step 6: Run your Python scripts to upload data  
Open a second terminal and run:  
cd ~/BDM_Project-unstructured_p2  
python3 trusted_zone/storing_unstructured.py

This script connects to MinIO and uploads:  
- Cleaned Parquet posts  
- Associated images  
- metadata.json and failed_log.json
