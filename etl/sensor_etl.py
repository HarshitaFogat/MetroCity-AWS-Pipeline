import sys, math
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ── Job initialisation ────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET   = args['BUCKET_NAME']
RAW_PATH = f's3://{BUCKET}/raw/road_traffic_sensor_data.csv'
OUT_PATH = f's3://{BUCKET}/processed/sensor/'

# ── EXTRACT: Read CSV from S3 ─────────────────────────────────────────────
df = (spark.read
        .option('header', 'true')
        .option('inferSchema', 'true')
        .csv(RAW_PATH))
print(f'Extracted {df.count()} sensor rows.')

# ── TRANSFORM: Temporal decomposition ────────────────────────────────────
df = df.withColumn('DateTime',    F.to_timestamp('Date_Time', 'yyyy-MM-dd HH:mm:ss'))
df = df.withColumn('year',        F.year('DateTime'))
df = df.withColumn('month',       F.month('DateTime'))
df = df.withColumn('day',         F.dayofmonth('DateTime'))
df = df.withColumn('hour',        F.hour('DateTime'))
df = df.withColumn('quarter',     F.quarter('DateTime'))
df = df.withColumn('day_of_week', F.dayofweek('DateTime'))
df = df.withColumn('is_weekend',
    F.when(F.col('day_of_week').isin([1, 7]), 1).otherwise(0))
df = df.withColumn('is_peak_hour',
    F.when((F.col('hour') >= 7)  & (F.col('hour') <= 10), 1)
    .when((F.col('hour') >= 17) & (F.col('hour') <= 20), 1)
    .otherwise(0))

# ── TRANSFORM: Congestion Index (identical formula from your notebook) ────
# Formula: 0.6 * (Vehicle_Count / MAX_VOL) + 0.4 * (1 - Average_Speed / MAX_SPD)
MAX_VOL = df.agg(F.max('Vehicle_Count')).collect()[0][0]   # 494
MAX_SPD = df.agg(F.max('Average_Speed')).collect()[0][0]   # 79

df = df.withColumn('congestion_index',
    F.round(
        (0.6 * F.col('Vehicle_Count') / MAX_VOL) +
        (0.4 * (1.0 - F.col('Average_Speed') / MAX_SPD)),
    4))

# ── TRANSFORM: Speed and volume category bands ────────────────────────────
df = df.withColumn('speed_category',
    F.when(F.col('Average_Speed') < 20, 'Very Slow')
    .when(F.col('Average_Speed') < 40, 'Slow')
    .when(F.col('Average_Speed') < 60, 'Moderate')
    .when(F.col('Average_Speed') < 80, 'Fast')
    .otherwise('Very Fast'))

df = df.withColumn('volume_band',
    F.when(F.col('Vehicle_Count') <= 100, 'Very Low')
    .when(F.col('Vehicle_Count') <= 200, 'Low')
    .when(F.col('Vehicle_Count') <= 300, 'Medium')
    .when(F.col('Vehicle_Count') <= 400, 'High')
    .otherwise('Very High'))

# ── TRANSFORM: Time-of-day classification ─────────────────────────────────
time_of_day_udf = F.udf(lambda h: (
    'Morning Peak'   if 7  <= h <= 10 else
    'Midday'         if 11 <= h <= 13 else
    'Evening Peak'   if 17 <= h <= 20 else
    'Off-Peak Night' if h  >= 21 or h <= 5 else
    'Off-Peak Day'), StringType())
df = df.withColumn('time_of_day', time_of_day_udf(F.col('hour')))
df = df.withColumn('sensor_status', F.lit('Active'))

# ── LOAD: Write Parquet to processed/ prefix ──────────────────────────────
df.write.mode('overwrite').parquet(OUT_PATH)
print(f'Sensor ETL complete. {df.count()} rows written to {OUT_PATH}')
job.commit()
