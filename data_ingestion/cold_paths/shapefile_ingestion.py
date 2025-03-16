import geopandas as gpd
import os

shapefile_url = "https://opendata-ajuntament.barcelona.cat/data/dataset/808daafa-d9ce-48c0-925a-fa5afdb1ed41/resource/11851135-6919-4dcb-91ed-821e5e87a428/download"
output_dir = "landing_zone/structured"
output_file = os.path.join(output_dir, "administrative_units.geojson")

os.makedirs(output_dir, exist_ok=True)

def fetch_shapefile_data():
    """Load and process the shapefile."""
    gdf = gpd.read_file(shapefile_url)
    gdf.to_file(output_file, driver="GeoJSON")
    print(f"Shapefile data saved to {output_file}")

if __name__ == "__main__":
    fetch_shapefile_data()
