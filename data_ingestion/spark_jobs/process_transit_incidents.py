from pyspark.sql import SparkSession
from delta import DeltaTable
import requests
import xmltodict

# Initialize Spark session with Delta extensions
spark = SparkSession.builder \
    .appName("TransitIncidents") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

DELTA_PATH = "/data/delta/transit"  # Specify your Delta table path

def fetch_gml_data():
    """Fetch transit incidents from GML API and convert to DataFrame."""
    url = "https://www.gencat.cat/transit/opendata/incidenciesGML.xml"
    response = requests.get(url)
    gml_data = xmltodict.parse(response.content)

    incidents = []
    for feature in gml_data["wfs:FeatureCollection"]["gml:featureMember"]:
        data = feature["cite:mct2_v_afectacions_data"]
        incidents.append({
            "id": int(data["cite:identificador"]),
            "road": data["cite:carretera"],
            "cause": data["cite:causa"],
            "description": data["cite:descripcio"],
            "latitude": float(data["cite:geom"]["gml:Point"]["gml:coordinates"].split(",")[1]),
            "longitude": float(data["cite:geom"]["gml:Point"]["gml:coordinates"].split(",")[0]),
            "timestamp": data["cite:data"]
        })

    return spark.createDataFrame(incidents)

def save_to_delta():
    """Fetch incidents and store them in Delta Lake."""
    df = fetch_gml_data()

    if DeltaTable.isDeltaTable(spark, DELTA_PATH):
        delta_table = DeltaTable.forPath(spark, DELTA_PATH)
        delta_table.alias("old").merge(
            df.alias("new"), "old.id = new.id"
        ).whenMatchedUpdate(set={"road": "new.road", "cause": "new.cause", "description": "new.description",
                                 "latitude": "new.latitude", "longitude": "new.longitude", "timestamp": "new.timestamp"}
        ).whenNotMatchedInsertAll().execute()
    else:
        df.write
