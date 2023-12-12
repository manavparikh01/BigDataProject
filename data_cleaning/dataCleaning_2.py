from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("NYPD_Complaints_Cleaning").getOrCreate()

file_path = "complaints.csv"
df = spark.read.csv(sys.argv[1], header=True, inferSchema=True)

df.createOrReplaceTempView("nypd_complaints")

filtered_complaints = spark.sql("""
    SELECT CMPLNT_NUM, CMPLNT_FR_DT, CMPLNT_FR_TM, ADDR_PCT_CD, RPT_DT,
           KY_CD, OFNS_DESC, PD_CD, PD_DESC, CRM_ATPT_CPTD_CD, BORO_NM,
           SUSP_AGE_GROUP, SUSP_RACE, SUSP_SEX, Latitude, Longitude,
           VIC_AGE_GROUP, VIC_RACE, VIC_SEX
    FROM nypd_complaints
""")

filtered_complaints.createOrReplaceTempView("filtered_nypd_complaints")

unique_complaints = spark.sql("""
    SELECT *
    FROM filtered_nypd_complaints
    WHERE CMPLNT_NUM IN (
        SELECT CMPLNT_NUM
        FROM filtered_nypd_complaints
        GROUP BY CMPLNT_NUM
        HAVING COUNT(*) = 1
    ) AND TO_DATE(CMPLNT_FR_DT, 'MM/dd/yyyy') IS NOT NULL
""")

unique_complaints.createOrReplaceTempView("unique_valid_complaints")


updated_race_df = spark.sql("""
    SELECT *,
        CASE WHEN SUSP_RACE IS NULL THEN 'UNKNOWN' ELSE SUSP_RACE END AS SUSP_RACE_UPDATED
    FROM nypd_complaints
""")

updated_race_df.write.csv("cleanedData.csv")

spark.stop()