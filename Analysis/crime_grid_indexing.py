# PySpark SQL code to assign polygon ID to each record in crime dataset.
# The code compares the latitude/longitude of the crime and the min/max present in grids csv and assigns the polygon id.
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PolygonAssignment").getOrCreate()

dfGrids = spark.read.csv("grid_coordinates_half_size.csv", header=True, inferSchema=True)
dfCrimeDataset = spark.read.csv("cleanedData2.csv", header=True, inferSchema=True)

dfGrids.createOrReplaceTempView("Grids")
dfCrimeDataset.createOrReplaceTempView("Crime")

# SQL Query to assign polygon IDs
result = spark.sql("""
    SELECT C.*, G.ID as PolygonID
    FROM Crime C
    JOIN Grids G
    ON C.Latitude >= G.`Min Latitude` AND C.Latitude <= G.`Max Latitude`
    AND C.Longitude >= G.`Min Longitude` AND C.Longitude <= G.`Max Longitude`
""")

result.write.csv("cleanedData2.csv", header=True)
spark.stop()