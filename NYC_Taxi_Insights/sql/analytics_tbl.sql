CREATE OR REPLACE TABLE `nycab-insights-project.nycab_dwh.analytics_dashboard_tbl` AS (
WITH trip_metrics AS (
  SELECT
    -- Vendor information
    v.VendorID,
    v.vendor_name,
    
    -- Temporal data - Pickup times
    d.tpep_pickup_datetime,
    d.pickup_hour,
    d.pickup_day,
    d.pickup_month,
    d.pickup_year,
    d.pickup_weekday,

    -- Temporal data - Dropoff times (added)
    d.tpep_dropoff_datetime,
    d.dropoff_hour,
    d.dropoff_day,
    d.dropoff_month,
    d.dropoff_year,
    d.dropoff_weekday,
    
    -- Core trip information
    f.passenger_count,
    f.trip_distance,
    r.ratecode_name,
    
    -- Location information
    pick_up.Borough AS pickup_Borough,
    pick_up.Zone AS pickup_Zone,
    pick_up.service_zone AS pickup_service_zone,
    drop_off.Borough AS dropoff_Borough,
    drop_off.Zone AS dropoff_Zone,
    drop_off.service_zone AS dropoff_service_zone,
    
    -- Payment information
    pay.payment_name,
    f.fare_amount,
    f.extra,
    f.mta_tax,
    f.tip_amount,
    f.tolls_amount,
    f.improvement_surcharge,
    f.total_amount,
    f.congestion_surcharge,
    f.Airport_fee,
    
    -- Calculated metrics
    TIMESTAMP_DIFF(d.tpep_dropoff_datetime, d.tpep_pickup_datetime, MINUTE) as trip_duration_minutes,
    CASE 
        WHEN f.tip_amount > 0 THEN 1 
        ELSE 0 
    END as has_tip,
    CASE
        WHEN f.trip_distance > 0 THEN f.fare_amount / f.trip_distance
        ELSE 0
    END as fare_per_mile

  FROM `nycab-insights-project.nycab_dwh.fact_trips` f
  JOIN `nycab-insights-project.nycab_dwh.datetime_dim` d 
    ON f.datetime_id = d.datetime_id
  JOIN `nycab-insights-project.nycab_dwh.vendor_dim` v 
    ON v.VendorID = f.VendorID
  JOIN `nycab-insights-project.nycab_dwh.Ratecode_dim` r 
    ON r.RatecodeID = f.RatecodeID  
  JOIN `nycab-insights-project.nycab_dwh.location_dim` pick_up 
    ON pick_up.LocationID = f.PULocationID
  JOIN `nycab-insights-project.nycab_dwh.location_dim` drop_off 
    ON drop_off.LocationID = f.DOLocationID
  JOIN `nycab-insights-project.nycab_dwh.payment_type_dim` pay 
    ON pay.payment_type = f.payment_type
)

SELECT 
    *,
    -- Pickup time metrics
    AVG(fare_amount) OVER (PARTITION BY pickup_hour) as avg_pickup_hourly_fare,
    AVG(trip_distance) OVER (PARTITION BY pickup_hour) as avg_pickup_hourly_distance,
    COUNT(*) OVER (PARTITION BY pickup_Borough, pickup_Zone) as pickup_trips_per_zone,
    
    -- Dropoff time metrics (added)
    AVG(fare_amount) OVER (PARTITION BY dropoff_hour) as avg_dropoff_hourly_fare,
    AVG(trip_distance) OVER (PARTITION BY dropoff_hour) as avg_dropoff_hourly_distance,
    COUNT(*) OVER (PARTITION BY dropoff_Borough, dropoff_Zone) as dropoff_trips_per_zone,
    
    -- Passenger metrics
    AVG(tip_amount) OVER (PARTITION BY CAST(passenger_count AS STRING)) as avg_tip_by_passengers,
    
    -- Statistical metrics
    PERCENTILE_CONT(fare_amount, 0.5) OVER () as median_fare,
    PERCENTILE_CONT(trip_distance, 0.5) OVER () as median_distance,
    
    -- Pickup time period categorization
    CASE 
        WHEN pickup_hour BETWEEN 6 AND 10 THEN 'Morning Rush'
        WHEN pickup_hour BETWEEN 16 AND 19 THEN 'Evening Rush'
        WHEN pickup_hour >= 23 OR pickup_hour <= 4 THEN 'Night'
        ELSE 'Off Peak'
    END as pickup_time_period,
    
    -- Dropoff time period categorization (added)
    CASE 
        WHEN dropoff_hour BETWEEN 6 AND 10 THEN 'Morning Rush'
        WHEN dropoff_hour BETWEEN 16 AND 19 THEN 'Evening Rush'
        WHEN dropoff_hour >= 23 OR dropoff_hour <= 4 THEN 'Night'
        ELSE 'Off Peak'
    END as dropoff_time_period,
    
    -- Distance categorization
    CASE 
        WHEN trip_distance <= 1 THEN 'Very Short'
        WHEN trip_distance <= 3 THEN 'Short'
        WHEN trip_distance <= 5 THEN 'Medium'
        WHEN trip_distance <= 10 THEN 'Long'
        ELSE 'Very Long'
    END as distance_category

FROM trip_metrics
WHERE 
    trip_distance > 0 
    AND fare_amount > 0 
    AND total_amount > 0
    AND passenger_count > 0
    AND passenger_count <= 6
);