from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("NYPD_Complaints_Cleaning").getOrCreate()

df = spark.read.csv(sys.argv[1], header=True, inferSchema=True)

df.createOrReplaceTempView("your_data")

result_df = spark.sql("""
    SELECT polygonId, age_group_with_max_count, max_count,
    CASE age_group_with_max_count
        WHEN '<18' THEN IF(SUM(CASE WHEN vic_age_group_alias != 'UNKNOWN' THEN 1 ELSE 0 END) < max_count, 1, 7)
        WHEN '25-44' THEN IF(SUM(CASE WHEN vic_age_group_alias != 'UNKNOWN' THEN 1 ELSE 0 END) < max_count, 3, 7)
        WHEN '65+' THEN IF(SUM(CASE WHEN vic_age_group_alias != 'UNKNOWN' THEN 1 ELSE 0 END) < max_count, 5, 7)
        WHEN '18-24' THEN IF(SUM(CASE WHEN vic_age_group_alias != 'UNKNOWN' THEN 1 ELSE 0 END) < max_count, 2, 7)
        WHEN '45-64' THEN IF(SUM(CASE WHEN vic_age_group_alias != 'UNKNOWN' THEN 1 ELSE 0 END) < max_count, 4, 7)
    END AS assigned_age_group
    FROM (
        SELECT 
            polygonId,
            VIC_AGE_GROUP AS age_group_with_max_count,
            COUNT(*) AS max_count,
            VIC_AGE_GROUP AS vic_age_group_alias
        FROM your_data
        WHERE VIC_AGE_GROUP IN ('<18', '25-44', '65+', '18-24', '45-64')
        GROUP BY polygonId, VIC_AGE_GROUP
    ) tmp
    GROUP BY polygonId, age_group_with_max_count, max_count
""")

result_df.write.csv("age_analysis_updated")
spark.stop()