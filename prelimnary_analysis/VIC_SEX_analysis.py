from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("OffenseCountsBySex").getOrCreate()

df = spark.read.csv(sys.argv[1], header=True)  # Assuming the CSV has a header

df.createOrReplaceTempView("data")

query = """
    SELECT YEAR(to_date(RPT_DT, 'MM/dd/yyyy')) AS Year, VIC_SEX, COUNT(*) AS OffenseCount
    FROM data
    GROUP BY Year, VIC_SEX
    ORDER BY Year, OffenseCount DESC
"""

result = spark.sql(query)

result.write.csv("offenses_by_sex.csv", header=True)

spark.stop()