import requests
import xmltodict
import pandas as pd
import os

# Define file paths
url = "https://data.renfe.com/dataset/81f21f5f-b093-413d-92a8-e9f5e670cd6b/resource/5d987311-277c-454f-bfbc-f81a32326973/download/principales-indicadores-ambientales.xls"
output_dir = "landing_zone/structured"
output_file = os.path.join(output_dir, "environmental_indicators.csv")

os.makedirs(output_dir, exist_ok=True)

def fetch_environmental_data():
    """Fetch XML environmental data from Renfe Open Data API."""
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch environmental data: {response.status_code}")

    xml_data = xmltodict.parse(response.content)
    df = pd.json_normalize(xml_data['root']['record'])  # Adjust structure as needed
    df.to_csv(output_file, index=False)
    print(f"Environmental data saved to {output_file}")

if __name__ == "__main__":
    fetch_environmental_data()
