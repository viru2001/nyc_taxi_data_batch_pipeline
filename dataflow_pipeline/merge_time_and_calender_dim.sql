MERGE nyc-taxi-batch-dataflow.trip_data.calendar_dim T
USING (
  SELECT DISTINCT date_id, date, year, quarter, month, day, day_of_week
  FROM nyc-taxi-batch-dataflow.trip_data.calendar_dim_staging
) S
ON T.date_id = S.date_id
WHEN NOT MATCHED THEN
  INSERT (date_id, date, year, quarter, month, day, day_of_week)
  VALUES(S.date_id, S.date, S.year, S.quarter, S.month, S.day, S.day_of_week);

truncate table nyc-taxi-batch-dataflow.trip_data.calendar_dim_staging;

MERGE nyc-taxi-batch-dataflow.trip_data.time_dim T
USING (
  SELECT DISTINCT time_id, hour, minute, second, period
  FROM nyc-taxi-batch-dataflow.trip_data.time_dim_staging
) S
ON T.time_id = S.time_id
WHEN NOT MATCHED THEN
  INSERT (time_id, hour, minute, second, period)
  VALUES(S.time_id, S.hour, S.minute, S.second, S.period);

truncate table nyc-taxi-batch-dataflow.trip_data.time_dim_staging;