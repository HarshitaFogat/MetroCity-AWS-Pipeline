import boto3, json, os

glue   = boto3.client("glue")
BUCKET = os.environ["BUCKET_NAME"]

def lambda_handler(event, context):
    """
    Triggered by EventBridge when a new object is created in S3 raw/.
    Examines the S3 key and starts the appropriate Glue job.
    """
    print("Event received:", json.dumps(event))

    # Extract the S3 object key from the EventBridge event structure
    s3_key = event["detail"]["object"]["key"]
    print(f"New file detected in S3: {s3_key}")

    jobs_started = []

    # Route to correct Glue job based on filename
    if "sensor" in s3_key.lower():
        run = glue.start_job_run(
            JobName  = "metrocity-sensor-etl",
            Arguments= {"--BUCKET_NAME": BUCKET}
        )
        jobs_started.append(f"metrocity-sensor-etl (RunId: {run['JobRunId']})")

    if "accident" in s3_key.lower():
        run = glue.start_job_run(
            JobName  = "metrocity-accident-etl",
            Arguments= {"--BUCKET_NAME": BUCKET}
        )
        jobs_started.append(f"metrocity-accident-etl (RunId: {run['JobRunId']})")

    if not jobs_started:
        print(f"No matching Glue job for key: {s3_key}. Skipping.")
        return {"statusCode": 200, "body": "No matching job."}

    msg = "Started jobs: " + ", ".join(jobs_started)
    print(msg)
    return {"statusCode": 200, "body": msg}
