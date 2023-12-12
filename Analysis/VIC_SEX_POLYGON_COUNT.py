#PySpark SQL code that:
#1. Gives victim-based crime count (male and female) for each polygon.
#2. Comparison of crime-count for both the genders & asigns colour accordingly for visualization.
#3. Joins the grid csv to add the min/max coordinates to the result & save it as csv.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark Session
spark = SparkSession.builder.appName("PolygonDataAnalysis").getOrCreate()

# Load your dataset into a DataFrame (assuming CSV for example)
data = spark.read.csv("cleanedData2.csv", header=True, inferSchema=True)
grid = spark.read.csv("grid_coordinates_half_size.csv", header=True, inferSchema=True)


# Register DataFrame as a temp table
data.createOrReplaceTempView("polygon_data")

# SQL Query
query = """
SELECT 
    PolygonID as ID,
    COUNT(*) as TOTAL_COUNT,
    SUM(CASE WHEN VIC_SEX = 'M' THEN 1 ELSE 0 END) as MALE,
    SUM(CASE WHEN VIC_SEX = 'F' THEN 1 ELSE 0 END) as FEMALE
FROM 
    polygon_data
GROUP BY 
    PolygonID
ORDER BY
    PolygonID
"""

# Run the query
result = spark.sql(query)

result_df = result.withColumn(
    "COLOUR",
    when(col("FEMALE") >= col("MALE") * 1.5, 0)
    .when((col("FEMALE") < col("MALE") * 1.5) & (col("FEMALE") >= col("MALE") * 1.25), 1)
    .when((col("MALE") < col("FEMALE") * 1.5) & (col("FEMALE") >= col("MALE") * 1.25), 4)
    .when(col("MALE") >= col("FEMALE") * 1.5, 4)
    .otherwise(2)
)

joined_df = result_df.join(grid, "ID").orderBy("ID")

# Write the result to a CSV file
joined_df.write.csv("vic_sex_polygon.csv", header=True)

spark.stop()