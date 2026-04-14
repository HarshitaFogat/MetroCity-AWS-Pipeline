import pandas as pd, boto3, json
from io import BytesIO
from sqlalchemy import create_engine

# ── Connection settings ───────────────────────────────────────────────────
RDS_HOST = "metrocity-db.cxw8cigc83qs.ap-south-1.rds.amazonaws.com"
RDS_USER = "admin"
RDS_PASS = "MetroCity2024!"
RDS_DB   = "smart_city_dw"
BUCKET   = "metrocity-harshita-data"

engine = create_engine(f"mysql+pymysql://{RDS_USER}:{RDS_PASS}@{RDS_HOST}/{RDS_DB}")
# ── Connection settings AWS Secrets Manager ───────────────────────────────────────────────────
# import boto3, json
# from sqlalchemy import create_engine

# client = boto3.client('secretsmanager')

# secret = json.loads(
#     client.get_secret_value(
#         SecretId='metrocity/rds/credentials'
#     )['SecretString']
# )

# engine = create_engine(
#     f"mysql+pymysql://{secret['username']}:{secret['password']}"
#     f"@{secret['host']}/{secret['dbname']}"
# )
# ── Read Parquet files from S3 ────────────────────────────────────────────
s3 = boto3.client("s3")

def read_parquet_s3(prefix):
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    dfs  = []
    for obj in resp.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            data = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
            dfs.append(pd.read_parquet(BytesIO(data)))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

sensor   = read_parquet_s3("processed/sensor/")
accident = read_parquet_s3("processed/accident/")
print(f"Read {len(sensor)} sensor rows, {len(accident)} accident rows from S3.")

# ── Load dim_location ─────────────────────────────────────────────────────
ZONE_MAP = {
    "Downtown":        "Urban Core",
    "Highway":         "Highway",
    "Residential Zone":"Residential",
    "Industrial Area": "Industrial",
    "Suburbs":         "Suburban"
}
loc_df = pd.DataFrame([{"location_name": k, "zone_type": v} for k,v in ZONE_MAP.items()])
loc_df.to_sql("dim_location", engine, if_exists="append", index=False)
loc_map = pd.read_sql("SELECT location_id, location_name FROM dim_location", engine)
loc_map = dict(zip(loc_map.location_name, loc_map.location_id))
print("dim_location loaded.")

# ── Load dim_vehicle ──────────────────────────────────────────────────────
VEH_CLASS = {"Bicycle":"Non-Motorised","Motorcycle":"Two-Wheeler",
            "Car":"Light Motor","Bus":"Heavy Motor","Truck":"Heavy Motor"}
veh_df = pd.DataFrame([{"vehicle_type":k,"vehicle_class":v} for k,v in VEH_CLASS.items()])
veh_df.to_sql("dim_vehicle", engine, if_exists="append", index=False)
veh_map = pd.read_sql("SELECT vehicle_id, vehicle_type FROM dim_vehicle", engine)
veh_map = dict(zip(veh_map.vehicle_type, veh_map.vehicle_id))
print("dim_vehicle loaded.")

# ── Load dim_weather ──────────────────────────────────────────────────────
ADVERSE = {"Fog","Snow","Storm"}
wthr_df = pd.DataFrame([{"weather_condition":w,"is_adverse":int(w in ADVERSE)}
                        for w in sorted(accident["Weather_Condition"].unique())])
wthr_df.to_sql("dim_weather", engine, if_exists="append", index=False)
wthr_map = pd.read_sql("SELECT weather_id, weather_condition FROM dim_weather", engine)
wthr_map = dict(zip(wthr_map.weather_condition, wthr_map.weather_id))
print("dim_weather loaded.")

# ── Load dim_date (union all timestamps from both datasets) ───────────────
import numpy as np
all_ts = pd.to_datetime(pd.concat([sensor["Date_Time"], accident["Date_Time"]]).unique())
date_rows = []
for ts in sorted(all_ts):
    date_rows.append({
        "full_datetime":   ts, "date_only": ts.date(),
        "year": ts.year, "quarter": ts.quarter, "month": ts.month,
        "month_name": ts.strftime("%B"), "month_sort": ts.month,
        "week": ts.isocalendar().week, "day_name": ts.strftime("%A"),
        "day_of_week_num": ts.isoweekday(),
        "hour": ts.hour,
        "time_of_day": ("Morning Peak" if 7<=ts.hour<=10 else
                        "Midday" if 11<=ts.hour<=13 else
                        "Evening Peak" if 17<=ts.hour<=20 else
                        "Off-Peak Night" if ts.hour>=21 or ts.hour<=5 else "Off-Peak Day"),
        "is_peak_hour": int(7<=ts.hour<=10 or 17<=ts.hour<=20),
        "is_weekend":   int(ts.isoweekday() >= 6)
    })
date_df = pd.DataFrame(date_rows)
date_df.to_sql("dim_date", engine, if_exists="append", index=False)
date_map = pd.read_sql("SELECT date_id, full_datetime FROM dim_date", engine)
date_map["full_datetime"] = pd.to_datetime(date_map["full_datetime"])
date_map = dict(zip(date_map.full_datetime, date_map.date_id))
print(f"dim_date loaded: {len(date_df)} rows.")

# ── Load fact_traffic ─────────────────────────────────────────────────────
sensor["location_id"] = sensor["Location"].map(loc_map)
sensor["date_id"]     = pd.to_datetime(sensor["Date_Time"]).map(date_map)
ft_cols = ["sensor_id","location_id","date_id","vehicle_count","average_speed",
        "congestion_level","congestion_index","speed_category","volume_band","sensor_status"]
# Map column names from Glue output to RDS schema names
sensor_load = sensor.rename(columns={
    "Sensor_ID":"sensor_id","Vehicle_Count":"vehicle_count",
    "Average_Speed":"average_speed","Congestion_Level":"congestion_level"
})[ft_cols]
sensor_load.to_sql("fact_traffic", engine, if_exists="append", index=False)
print("fact_traffic loaded.")

# ── Load fact_accident ────────────────────────────────────────────────────
accident["location_id"] = accident["Location"].map(loc_map)
accident["vehicle_id"]  = accident["Vehicle_Type"].map(veh_map)
accident["weather_id"]  = accident["Weather_Condition"].map(wthr_map)
accident["date_id"]     = pd.to_datetime(accident["Date_Time"]).map(date_map)
fa_df = accident.rename(columns={
    "Accident_ID":"accident_id","Road_Condition":"road_condition",
    "Vehicle_Type":"vehicle_type","Weather_Condition":"weather_condition",
    "Accident_Severity":"accident_severity","Number_of_Vehicles":"number_of_vehicles",
    "Casualties":"casualties","Traffic_Density":"traffic_density"
})
fa_cols = ["accident_id","location_id","date_id","vehicle_id","weather_id",
        "vehicle_type","weather_condition","road_condition","accident_severity",
        "number_of_vehicles","casualties","traffic_density","severity_score",
        "risk_score","hazard_level","adverse_weather","hazardous_road",
        "is_peak_hour","is_fatal_or_severe","traffic_density_num"]
fa_df[fa_cols].to_sql("fact_accident", engine, if_exists="append", index=False)
print("fact_accident loaded.")

print("All tables loaded successfully.")

