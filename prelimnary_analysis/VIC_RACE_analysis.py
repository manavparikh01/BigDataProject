from pyspark.sql import SparkSession
import sys
spark = SparkSession.builder.appName("OffenseCountsByRace").getOrCreate()

df = spark.read.csv(sys.argv[1], header=True)

df.createOrReplaceTempView("data")

query = """
    SELECT YEAR(to_date(RPT_DT, 'MM/dd/yyyy')) AS Year, VIC_RACE, COUNT(*) AS OffenseCount
    FROM data
    GROUP BY Year, VIC_RACE
    ORDER BY Year, OffenseCount DESC
"""

result = spark.sql(query)

result.write.csv("offenses_by_race.csv", header=True)

spark.stop()