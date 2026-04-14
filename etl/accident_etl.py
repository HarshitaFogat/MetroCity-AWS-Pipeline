import sys, math
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET   = args['BUCKET_NAME']
RAW_PATH = f's3://{BUCKET}/raw/traffic_accident_data.csv'
OUT_PATH = f's3://{BUCKET}/processed/accident/'

# ── EXTRACT ───────────────────────────────────────────────────────────────
df = (spark.read.option('header','true').option('inferSchema','true').csv(RAW_PATH))
print(f'Extracted {df.count()} accident rows.')

# ── TRANSFORM: Temporal decomposition (same as sensor job) ───────────────
df = df.withColumn('DateTime',    F.to_timestamp('Date_Time', 'yyyy-MM-dd HH:mm:ss'))
df = df.withColumn('year',        F.year('DateTime'))
df = df.withColumn('month',       F.month('DateTime'))
df = df.withColumn('day',         F.dayofmonth('DateTime'))
df = df.withColumn('hour',        F.hour('DateTime'))
df = df.withColumn('quarter',     F.quarter('DateTime'))
df = df.withColumn('day_of_week', F.dayofweek('DateTime'))
df = df.withColumn('is_weekend',  F.when(F.col('day_of_week').isin([1,7]),1).otherwise(0))
df = df.withColumn('is_peak_hour',
    F.when((F.col('hour')>=7)&(F.col('hour')<=10),1)
    .when((F.col('hour')>=17)&(F.col('hour')<=20),1).otherwise(0))

# ── TRANSFORM: Severity score (same mapping as original notebook) ─────────
# Minor=1, Moderate=2, Severe=3, Fatal=4
df = df.withColumn('severity_score',
    F.when(F.col('Accident_Severity')=='Minor',    1)
    .when(F.col('Accident_Severity')=='Moderate', 2)
    .when(F.col('Accident_Severity')=='Severe',   3)
    .otherwise(4))

# ── TRANSFORM: Risk Score: severity * log1p(casualties+1) * num_vehicles ──
# WHY log1p(casualties+1): ensures zero-casualty Fatal accidents still score
# above zero. log1p(0+1) = 0.693 gives a meaningful floor to Fatal events.
log1p_udf = F.udf(lambda x: float(math.log1p((x or 0) + 1)), DoubleType())
df = df.withColumn('log_cas',    log1p_udf(F.col('Casualties')))
df = df.withColumn('risk_score',
    F.round(F.col('severity_score') * F.col('log_cas') * F.col('Number_of_Vehicles'), 4))

# ── TRANSFORM: Hazard level bucketing ────────────────────────────────────
df = df.withColumn('hazard_level',
    F.when(F.col('risk_score') <= 5,  'Low Hazard')
    .when(F.col('risk_score') <= 15, 'Moderate Hazard')
    .when(F.col('risk_score') <= 30, 'High Hazard')
    .otherwise('Critical Hazard'))

# ── TRANSFORM: Binary flags ───────────────────────────────────────────────
df = df.withColumn('adverse_weather',
    F.when(F.col('Weather_Condition').isin(['Fog','Snow','Storm']),1).otherwise(0))
df = df.withColumn('hazardous_road',
    F.when(F.col('Road_Condition').isin(['Icy','Under Construction']),1).otherwise(0))
df = df.withColumn('is_fatal_or_severe',
    F.when(F.col('Accident_Severity').isin(['Fatal','Severe']),1).otherwise(0))
df = df.withColumn('traffic_density_num',
    F.when(F.col('Traffic_Density')=='Low',1)
    .when(F.col('Traffic_Density')=='Moderate',2).otherwise(3))

# ── TRANSFORM: Vehicle class grouping ────────────────────────────────────
veh_class_udf = F.udf(lambda v: {
    'Bicycle':'Non-Motorised','Motorcycle':'Two-Wheeler',
    'Car':'Light Motor','Bus':'Heavy Motor','Truck':'Heavy Motor'
}.get(v,'Other'), StringType())
df = df.withColumn('vehicle_class', veh_class_udf(F.col('Vehicle_Type')))

# ── LOAD ──────────────────────────────────────────────────────────────────
df.write.mode('overwrite').parquet(OUT_PATH)
print(f'Accident ETL complete. {df.count()} rows written to {OUT_PATH}')
job.commit()