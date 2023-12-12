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

filtered_complaints = spark.sql("""
    SELECT *
    FROM unique_valid_complaints
    WHERE Longitude <= -70 AND Longitude >= -76
""")

filtered_complaints.createOrReplaceTempView("longitude_filtered_complaints")

filtered_complaints = spark.sql("""
    SELECT *
    FROM longitude_filtered_complaints
    WHERE Latitude >= 40 AND Latitude <= 42
""")

filtered_complaints.createOrReplaceTempView("nypd_complaints")

updated_race_df = spark.sql("""
    SELECT *,
        CASE WHEN SUSP_RACE IS NULL THEN 'UNKNOWN' ELSE SUSP_RACE END AS SUSP_RACE_UPDATED
    FROM nypd_complaints
""")

updated_race_df.createOrReplaceTempView("nypd_complaints")


count_ofns = spark.sql("""
    SELECT KY_CD
    FROM nypd_complaints
    GROUP BY KY_CD
    HAVING COUNT(DISTINCT OFNS_DESC) =  2
""")
count_ofns.createOrReplaceTempView("count_ofns")

updated_data = spark.sql("""
    SELECT 
        c.CMPLNT_NUM, c.CMPLNT_FR_DT, c.CMPLNT_FR_TM, c.ADDR_PCT_CD, c.RPT_DT,
        c.KY_CD, 
        CASE 
            WHEN c.OFNS_DESC = '(null)' THEN nfns.non_null_ofns_desc
            ELSE c.OFNS_DESC
        END AS OFNS_DESC,
        c.PD_CD, c.PD_DESC, c.CRM_ATPT_CPTD_CD, c.BORO_NM,
        c.SUSP_AGE_GROUP, c.SUSP_RACE, c.SUSP_SEX, c.Latitude, c.Longitude,
        c.VIC_AGE_GROUP, c.VIC_RACE, c.VIC_SEX
    FROM 
        nypd_complaints c
    JOIN 
        (
            SELECT KY_CD, MAX(OFNS_DESC) AS non_null_ofns_desc
            FROM nypd_complaints
            WHERE OFNS_DESC IS NOT NULL AND OFNS_DESC != '(null)'
            GROUP BY KY_CD
        ) nfns
    ON 
        c.KY_CD = nfns.KY_CD
    LEFT JOIN 
        count_ofns co
    ON 
        c.KY_CD = co.KY_CD
    WHERE 
        co.KY_CD IS NOT NULL
""")

updated_data.createOrReplaceTempView("nypd_complaints")

mapped_ofns_desc = spark.sql("""
    SELECT c.CMPLNT_NUM, c.CMPLNT_FR_DT, c.CMPLNT_FR_TM, c.ADDR_PCT_CD, c.RPT_DT,
        c.KY_CD, 
        CASE 
            WHEN KY_CD = 124 THEN 'KIDNAPPING & RELATED OFFENSES'
            WHEN KY_CD = 116 THEN 'SEX CRIMES'
            WHEN KY_CD = 125 THEN 'NYS LAWS-UNCLASSIFIED FELONY'
            WHEN KY_CD = 364 THEN 'OTHER STATE LAWS (NON PENAL LAW)'
            WHEN KY_CD = 343 THEN 'THEFT OF SERVICES'
            ELSE OFNS_DESC
        END AS OFNS_DESC,
        c.PD_CD, c.PD_DESC, c.CRM_ATPT_CPTD_CD, c.BORO_NM,
        c.SUSP_AGE_GROUP, c.SUSP_RACE, c.SUSP_SEX, c.Latitude, c.Longitude,
        c.VIC_AGE_GROUP, c.VIC_RACE, c.VIC_SEX
    FROM nypd_complaints c
""")
updated_data.createOrReplaceTempView("nypd_complaints")

filtered_data = spark.sql("""
    SELECT *
    FROM nypd_complaints
    WHERE SUSP_AGE_GROUP IN ('<18', '25-44', '(null)', 'UNKNOWN', '65+', '18-24', '45-64')
""")
filtered_data.createOrReplaceTempView("nypd_complaints")
filtered_data = spark.sql("""
SELECT
    CMPLNT_NUM, CMPLNT_FR_DT, CMPLNT_FR_TM, ADDR_PCT_CD, RPT_DT,
    KY_CD, OFNS_DESC, PD_CD, PD_DESC, CRM_ATPT_CPTD_CD, BORO_NM,
    CASE 
        WHEN SUSP_AGE_GROUP = '(null)' THEN 'UNKNOWN'
        ELSE SUSP_AGE_GROUP
    END AS SUSP_AGE_GROUP,
    SUSP_RACE, SUSP_SEX, Latitude, Longitude,
    VIC_AGE_GROUP, VIC_RACE, VIC_SEX
FROM nypd_complaints
""")
filtered_data.createOrReplaceTempView("nypd_complaints")
filtered_data = spark.sql("""
SELECT
    CMPLNT_NUM, CMPLNT_FR_DT, CMPLNT_FR_TM, ADDR_PCT_CD, RPT_DT,
    KY_CD, OFNS_DESC, PD_CD, PD_DESC, CRM_ATPT_CPTD_CD, BORO_NM,SUSP_AGE_GROUP,
    CASE 
        WHEN SUSP_RACE = '(null)' THEN 'UNKNOWN'
        ELSE SUSP_RACE
    END AS SUSP_RACE,
    SUSP_SEX, Latitude, Longitude,
    VIC_AGE_GROUP, VIC_RACE, VIC_SEX
FROM nypd_complaints
""")
filtered_data.write.csv("filtered_cleanedData.csv", header=True)
spark.stop()