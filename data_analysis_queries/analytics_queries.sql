-- total amount per payment type
select
    tr.payment_type,
    pt.payment_type_name,
    sum(tr.total_amount) as total_amount
from
    (
        select
            payment_type,
            total_amount
        from
            nyc - taxi - batch - dataflow.trip_data.trip_records
        where
            trip_date >= '2023-01-01'
    ) tr
    left join nyc - taxi - batch - dataflow.trip_data.payment_method_dim pt on tr.payment_type = pt.payment_type
group by
    tr.payment_type,
    pt.payment_type_name



-- total number of trips per day of the week
select
    cal.day_of_week,
    count(*) as number_of_trips
from
    (
        select
            pickup_date_id
        from
            nyc - taxi - batch - dataflow.trip_data.trip_records
        where
            trip_date >= '2023-01-01'
    ) tr
    left join nyc - taxi - batch - dataflow.trip_data.calendar_dim cal on tr.pickup_date_id = cal.date_id
group by
    cal.day_of_week



-- total number of trips per month
select
    cal.year,
    FORMAT_DATE (
        '%B',
        PARSE_DATE ('%m', CAST(cal.month AS STRING))
    ) AS month_name,
    count(*) as number_of_trips
from
    (
        select
            pickup_date_id
        from
            nyc - taxi - batch - dataflow.trip_data.trip_records
        where
            trip_date >= '2023-01-01'
    ) tr
    left join nyc - taxi - batch - dataflow.trip_data.calendar_dim cal on tr.pickup_date_id = cal.date_id
group by
    cal.month,
    cal.year




-- avaerage fare amount per month
select
    cal.year,
    FORMAT_DATE (
        '%B',
        PARSE_DATE ('%m', CAST(cal.month AS STRING))
    ) AS month_name,
    round(avg(tr.fare_amount), 2) as average_fare_amount
from
    (
        select
            pickup_date_id,
            fare_amount
        from
            nyc - taxi - batch - dataflow.trip_data.trip_records
        where
            trip_date >= '2023-01-01'
    ) tr
    left join nyc - taxi - batch - dataflow.trip_data.calendar_dim cal on tr.pickup_date_id = cal.date_id
group by
    cal.month,
    cal.year




-- average trip distance per month
select
    cal.year,
    FORMAT_DATE (
        '%B',
        PARSE_DATE ('%m', CAST(cal.month AS STRING))
    ) AS month_name,
    round(avg(tr.trip_distance), 2) as average_trip_distance
from
    (
        select
            pickup_date_id,
            trip_distance
        from
            nyc - taxi - batch - dataflow.trip_data.trip_records
        where
            trip_date >= '2023-01-01'
    ) tr
    left join nyc - taxi - batch - dataflow.trip_data.calendar_dim cal on tr.pickup_date_id = cal.date_id
group by
    cal.month,
    cal.year





-- top 10 pickup locations
select
    zone_dim.zone,
    count(*) as number_of_trips
from
    (
        select
            pickup_location_id
        from
            nyc - taxi - batch - dataflow.trip_data.trip_records
        where
            trip_date >= '2023-01-01'
    ) tr
    left join nyc - taxi - batch - dataflow.trip_data.taxi_zone_dim zone_dim on zone_dim.location_id = tr.pickup_location_id
group by
    zone_dim.zone
order by
    number_of_trips desc
limit
    10





-- top 10 dropoff locations
select
    zone_dim.zone,
    count(*) as number_of_trips
from
    (
        select
            dropoff_location_id
        from
            nyc - taxi - batch - dataflow.trip_data.trip_records
        where
            trip_date >= '2023-01-01'
    ) tr
    left join nyc - taxi - batch - dataflow.trip_data.taxi_zone_dim zone_dim on zone_dim.location_id = tr.dropoff_location_id
group by
    zone_dim.zone
order by
    number_of_trips desc
limit
    10




-- Most Lucrative Routes (Highest Average Fare):
-- routes with highest Profitability
select
    zone_dim1.zone as pickup_location,
    zone_dim2.zone as dropoff_location,
    round(avg(tr.fare_amount), 2) as average_fare_amount
from
    (
        select
            pickup_location_id,
            dropoff_location_id,
            fare_amount
        from
            nyc - taxi - batch - dataflow.trip_data.trip_records
        where
            trip_date >= '2023-01-01'
    ) tr
    left join nyc - taxi - batch - dataflow.trip_data.taxi_zone_dim zone_dim1 on zone_dim1.location_id = tr.pickup_location_id
    left join nyc - taxi - batch - dataflow.trip_data.taxi_zone_dim zone_dim2 on zone_dim2.location_id = tr.dropoff_location_id
group by
    pickup_location,
    dropoff_location
order by
    average_fare_amount desc
limit
    10




-- Visualization: Table or bar chart, joining with location names for readability.
-- Popular Routes (Pick-Up to Drop-Off):
select
    zone_dim1.zone as pickup_location,
    zone_dim2.zone as dropoff_location,
    count(*) as number_of_trips
from
    (
        select
            pickup_location_id,
            dropoff_location_id
        from
            nyc - taxi - batch - dataflow.trip_data.trip_records
        where
            trip_date >= '2023-01-01'
    ) tr
    left join nyc - taxi - batch - dataflow.trip_data.taxi_zone_dim zone_dim1 on zone_dim1.location_id = tr.pickup_location_id
    left join nyc - taxi - batch - dataflow.trip_data.taxi_zone_dim zone_dim2 on zone_dim2.location_id = tr.dropoff_location_id
group by
    pickup_location,
    dropoff_location
order by
    number_of_trips desc
limit
    10




-- Average Fare by Pick-Up Location
select
    zone_dim1.zone as pickup_location,
    round(avg(tr.fare_amount), 2) as average_fare_amount
from
    (
        select
            pickup_location_id,
            fare_amount
        from
            nyc - taxi - batch - dataflow.trip_data.trip_records
        where
            trip_date >= '2023-01-01'
    ) tr
    left join nyc - taxi - batch - dataflow.trip_data.taxi_zone_dim zone_dim1 on zone_dim1.location_id = tr.pickup_location_id
group by
    pickup_location
order by
    average_fare_amount desc
limit
    10




-- Busy Hours of a day:
SELECT
    EXTRACT(
        HOUR
        FROM
            pickup_datetime
    ) AS hour,
    COUNT(*) AS trip_count
from
    nyc - taxi - batch - dataflow.trip_data.trip_records
where
    trip_date >= '2023-01-01'
GROUP BY
    hour
ORDER BY
    hour;




-- Weekday vs. Weekend Trips:
-- Compares trip volumes on weekdays and weekends.
SELECT
    CASE
        WHEN EXTRACT(
            DAYOFWEEK
            FROM
                pickup_datetime
        ) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    COUNT(*) AS trip_count
FROM
    nyc - taxi - batch - dataflow.trip_data.trip_records
where
    trip_date >= '2023-01-01'
GROUP BY
    day_type;



-- Peak vs. Off-Peak Fares:
-- Compares average fares during peak hours (e.g., 8am-10am, 4pm-6pm) vs. off-peak.
SELECT CASE
WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 8 AND 10 
OR EXTRACT(HOUR FROM pickup_datetime) BETWEEN 16 AND 18 THEN 'Peak'
ELSE 'Off-Peak'
END AS time_period, AVG(fare_amount) AS avg_fare
FROM nyc-taxi-batch-dataflow.trip_data.trip_records where trip_date >= '2023-01-01'
GROUP BY time_period;