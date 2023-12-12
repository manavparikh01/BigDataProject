from pyspark.sql import SparkSession
import sys

# Initialize Spark session
spark = SparkSession.builder.appName("NYPD_Complaints_Cleaning").getOrCreate()

# Load the dataset (replace 'file_path' with your dataset path)
df = spark.read.csv(sys.argv[1], header=True, inferSchema=True)

# Register DataFrame as a temporary view
df.createOrReplaceTempView("your_data")

# SQL query to find the VIC_RACE with the maximum count for each polygonId
result_df = spark.sql("""
    SELECT polygonId, race_with_max_count, max_count,
    CASE race_with_max_count
        WHEN 'WHITE' THEN IF(SUM(CASE WHEN vic_race_alias != 'UNKNOWN' THEN 1 ELSE 0 END) < max_count, 1, 7)
        WHEN 'BLACK' THEN IF(SUM(CASE WHEN vic_race_alias != 'UNKNOWN' THEN 1 ELSE 0 END) < max_count, 2, 7)
        WHEN 'AMERICAN INDIAN/ALASKAN NATIVE' THEN IF(SUM(CASE WHEN vic_race_alias != 'UNKNOWN' THEN 1 ELSE 0 END) < max_count, 3, 7)
        WHEN 'BLACK HISPANIC' THEN IF(SUM(CASE WHEN vic_race_alias != 'UNKNOWN' THEN 1 ELSE 0 END) < max_count, 4, 7)
        WHEN 'WHITE HISPANIC' THEN IF(SUM(CASE WHEN vic_race_alias != 'UNKNOWN' THEN 1 ELSE 0 END) < max_count, 5, 7)
        WHEN 'ASIAN / PACIFIC ISLANDER' THEN IF(SUM(CASE WHEN vic_race_alias != 'UNKNOWN' THEN 1 ELSE 0 END) < max_count, 6, 7)
    END AS assigned_race
    FROM (
        SELECT 
            polygonId,
            VIC_RACE AS race_with_max_count,
            COUNT(*) AS max_count,
            VIC_RACE AS vic_race_alias
        FROM your_data
        WHERE VIC_RACE IN ('WHITE', 'BLACK', 'AMERICAN INDIAN/ALASKAN NATIVE', 'BLACK HISPANIC', 'WHITE HISPANIC', 'ASIAN / PACIFIC ISLANDER')
        GROUP BY polygonId, VIC_RACE
    ) tmp
    GROUP BY polygonId, race_with_max_count, max_count
""")

# Write the result DataFrame to a file
result_df.write.csv("race_analysis_updated")
spark.stop()