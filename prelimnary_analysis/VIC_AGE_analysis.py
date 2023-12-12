from pyspark.sql import SparkSession
import sys
spark = SparkSession.builder.appName("OffenseCountsByAgeGroup").getOrCreate()

df = spark.read.csv(sys.argv[1], header=True)
df.createOrReplaceTempView("data")

query = """
    SELECT YEAR(to_date(RPT_DT, 'MM/dd/yyyy')) AS Year, VIC_AGE_GROUP, COUNT(*) AS OffenseCount
    FROM data
    GROUP BY Year, VIC_AGE_GROUP
    ORDER BY Year, OffenseCount DESC
"""

result = spark.sql(query)
result.write.csv("offenses_by_age_group_v2.csv", header=True)
spark.stop()