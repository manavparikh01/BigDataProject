from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("NYPD_Complaints_Cleaning").getOrCreate()

df = spark.read.csv(sys.argv[1], header=True, inferSchema=True)
df.createOrReplaceTempView("nypd_complaints")

yearly_crime_distribution_query = spark.sql("""
    SELECT YEAR(TO_DATE(RPT_DT, 'MM/dd/yyyy')) AS ReportYear, OFNS_DESC, COUNT(*) AS CrimeCount
    FROM nypd_complaints
    WHERE RPT_DT IS NOT NULL
    GROUP BY ReportYear, OFNS_DESC
    ORDER BY ReportYear, OFNS_DESC
""")

yearly_crime_distribution_query.write.mode("overwrite").csv("yearly_crime_distribution_per_type_sql")

spark.stop()