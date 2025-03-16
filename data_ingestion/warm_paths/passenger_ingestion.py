import pandas as pd
import os

csv_url = "https://data.renfe.com/dataset/9190f983-e138-42da-901a-b37205562fe4/resource/1417396e-4d6a-466a-a987-03d07aa92bed/download/barcelona_viajeros_por_franja_csv.csv"
output_dir = "landing_zone/structured"
output_file = os.path.join(output_dir, "passenger_volume.csv")

os.makedirs(output_dir, exist_ok=True)

def fetch_passenger_data():
    """Fetch CSV data from Renfe Open Data."""
    df = pd.read_csv(csv_url)
    df.to_csv(output_file, index=False)
    print(f"Passenger volume data saved to {output_file}")

if __name__ == "__main__":
    fetch_passenger_data()
