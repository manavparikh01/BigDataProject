from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("NYPD_Complaints_Cleaning").getOrCreate()

file_path = "complaints.csv"
df = spark.read.csv(sys.argv[1], header=True, inferSchema=True)

df.createOrReplaceTempView("nypd_complaints")

valid_complaint_date = spark.sql("""
    SELECT CMPLNT_NUM, COUNT(*) AS InvalidDateCount
    FROM nypd_complaints
    WHERE TO_DATE(CMPLNT_FR_DT, 'MM/dd/yyyy') IS NULL
    GROUP BY CMPLNT_NUM
""")
valid_complaint_date.write.csv("invalid_complaint_from_date")

valid_complaint_date = spark.sql("""
    SELECT CMPLNT_NUM, COUNT(*) AS InvalidDateCount
    FROM nypd_complaints
    WHERE TO_DATE(CMPLNT_TO_DT, 'MM/dd/yyyy') IS NULL
    GROUP BY CMPLNT_NUM
""")
valid_complaint_date.write.csv("invalid_complaint_to_date")


valid_complaint_date = spark.sql("""
    SELECT CMPLNT_NUM, COUNT(*) AS InvalidDateCount
    FROM nypd_complaints
    WHERE TO_DATE(RPT_DT, 'MM/dd/yyyy') IS NULL
    GROUP BY CMPLNT_NUM
""")
valid_complaint_date.write.csv("invalid_report_date")

invalid_date_count = spark.sql("""
    SELECT COUNT(*) AS TotalInvalidDateCount
    FROM nypd_complaints
    WHERE TO_DATE(RPT_DT, 'MM/dd/yyyy') IS NULL
    OR TO_DATE(CMPLNT_FR_DT, 'MM/dd/yyyy') IS NULL
    OR TO_DATE(CMPLNT_TO_DT, 'MM/dd/yyyy') IS NULL
""")
invalid_date_count.write.csv("total_invalid_dates_count")


ky_cd_ofns_desc = spark.sql("""
    SELECT KY_CD, COUNT(DISTINCT OFNS_DESC) AS NumOfnsDesc
    FROM nypd_complaints
    GROUP BY KY_CD
    HAVING COUNT(DISTINCT OFNS_DESC) > 1
""")
ky_cd_ofns_desc.write.csv("different_offencecode_description")

ky_cd_ofns_desc = spark.sql("""
    SELECT PD_CD, COUNT(DISTINCT PD_DESC) AS NumOfnsDesc
    FROM nypd_complaints
    GROUP BY PD_CD
    HAVING COUNT(DISTINCT PD_DESC) > 1
""")
ky_cd_ofns_desc.write.csv("different_classigicationcode_description")


unique_crm_atpt_cptd_cd = spark.sql("SELECT DISTINCT CRM_ATPT_CPTD_CD FROM nypd_complaints")
unique_crm_atpt_cptd_cd.write.csv("disctinc_CRMATPTCPTD")

unique_crm_atpt_cptd_cd = spark.sql("SELECT DISTINCT LAW_CAT_CD FROM nypd_complaints")
unique_crm_atpt_cptd_cd.write.csv("disctinc_lawcatcd")


unique_crm_atpt_cptd_cd = spark.sql("SELECT DISTINCT BORO_NM FROM nypd_complaints")
unique_crm_atpt_cptd_cd.write.csv("disctinc_borough")


null_susp_age_count = spark.sql("""
    SELECT COUNT(*) AS NullSUSPAgeCount
    FROM nypd_complaints
    WHERE SUSP_AGE_GROUP IS NULL
""")
null_susp_age_count.write.csv("null_suspect_age")

distinct_susp_age_counts = spark.sql("""
    SELECT SUSP_AGE_GROUP, COUNT(*) AS CountOfSUSPAge
    FROM nypd_complaints
    WHERE SUSP_AGE_GROUP IS NOT NULL
    GROUP BY SUSP_AGE_GROUP
""")
distinct_susp_age_counts.write.csv("suspect_age_count")

null_susp_age_count = spark.sql("""
    SELECT COUNT(*) AS SUSP_RACECount
    FROM nypd_complaints
    WHERE SUSP_RACE IS NULL
""")
null_susp_age_count.write.csv("null_suspect_race")

distinct_susp_age_counts = spark.sql("""
    SELECT SUSP_RACE, COUNT(*) AS CountOfSUSPAge
    FROM nypd_complaints
    WHERE SUSP_RACE IS NOT NULL
    GROUP BY SUSP_RACE
""")
distinct_susp_age_counts.write.csv("suspect_race_count")

null_susp_age_count = spark.sql("""
    SELECT COUNT(*) AS SUSP_SEXCount
    FROM nypd_complaints
    WHERE SUSP_SEX IS NULL
""")
null_susp_age_count.write.csv("null_suspect_sex")

distinct_susp_age_counts = spark.sql("""
    SELECT SUSP_SEX, COUNT(*) AS CountOfSUSPAge
    FROM nypd_complaints
    WHERE SUSP_SEX IS NOT NULL
    GROUP BY SUSP_SEX
""")
distinct_susp_age_counts.write.csv("suspect_sex_count")

latitude_count = spark.sql("""
    SELECT COUNT(*) AS LatitudeCount
    FROM nypd_complaints
    WHERE Latitude < 40 OR Latitude > 42
""")
latitude_count.write.csv("latitude_outlier")

longitude_count = spark.sql("""
    SELECT COUNT(*) AS LongitudeCount
    FROM nypd_complaints
    WHERE Longitude > -70 OR Longitude < -76
""")
longitude_count.write.csv("longitude_outlier")
spark.stop()