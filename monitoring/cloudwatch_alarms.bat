@echo off

REM Set variables
set ACCOUNT_ID=750620721113
set REGION=ap-south-1
set TOPIC_ARN=arn:aws:sns:ap-south-1:750620721113:metrocity-pipeline-alerts

REM Alarm 1: Sensor ETL failure
aws cloudwatch put-metric-alarm ^
--alarm-name "MetroCity-SensorETL-Failed" ^
--alarm-description "Alert when sensor ETL Glue job fails" ^
--namespace "Glue" ^
--metric-name "glue.driver.aggregate.numFailedTasks" ^
--dimensions Name=JobName,Value=metrocity-sensor-etl ^
--statistic Sum ^
--period 300 ^
--threshold 1 ^
--comparison-operator GreaterThanOrEqualToThreshold ^
--evaluation-periods 1 ^
--alarm-actions %TOPIC_ARN% ^
--treat-missing-data notBreaching

REM Alarm 2: Accident ETL failure
aws cloudwatch put-metric-alarm ^
--alarm-name "MetroCity-AccidentETL-Failed" ^
--alarm-description "Alert when accident ETL Glue job fails" ^
--namespace "Glue" ^
--metric-name "glue.driver.aggregate.numFailedTasks" ^
--dimensions Name=JobName,Value=metrocity-accident-etl ^
--statistic Sum ^
--period 300 ^
--threshold 1 ^
--comparison-operator GreaterThanOrEqualToThreshold ^
--evaluation-periods 1 ^
--alarm-actions %TOPIC_ARN% ^
--treat-missing-data notBreaching

REM Alarm 3: Lambda pipeline trigger errors
aws cloudwatch put-metric-alarm ^
--alarm-name "MetroCity-Lambda-Errors" ^
--alarm-description "Alert when pipeline trigger Lambda has errors" ^
--namespace "AWS/Lambda" ^
--metric-name "Errors" ^
--dimensions Name=FunctionName,Value=metrocity-pipeline-trigger ^
--statistic Sum ^
--period 300 ^
--threshold 1 ^
--comparison-operator GreaterThanOrEqualToThreshold ^
--evaluation-periods 1 ^
--alarm-actions %TOPIC_ARN% ^
--treat-missing-data notBreaching

REM Verify all alarms
aws cloudwatch describe-alarms ^
--alarm-name-prefix "MetroCity" ^
--query "MetricAlarms[].{Name:AlarmName, State:StateValue}"

echo.
echo All alarms configured successfully!
pause