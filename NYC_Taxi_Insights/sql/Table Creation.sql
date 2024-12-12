CREATE OR REPLACE TABLE `nycab-insights-project.nycab_dwh.location_dim` (
    LocationID INT64 NOT NULL,
    Borough STRING,
    Zone STRING,
    service_zone STRING
);

CREATE OR REPLACE TABLE `nycab-insights-project.nycab_dwh.datetime_dim` (
datetime_id INT64,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    year_month STRING,
    pickup_hour INT64,
    pickup_day INT64,
    pickup_month INT64,
    pickup_year INT64,
    pickup_weekday INT64,
    pickup_date DATE,
    dropoff_hour INT64,
    dropoff_day INT64,
    dropoff_month INT64,
    dropoff_year INT64,
    dropoff_weekday INT64,
    dropoff_date DATE
);

CREATE OR REPLACE TABLE `nycab-insights-project.nycab_dwh.payment_type_dim` (
    payment_type INT64 NOT NULL,
    payment_name STRING
);

CREATE OR REPLACE TABLE `nycab-insights-project.nycab_dwh.Ratecode_dim` (
    RatecodeID INT64 NOT NULL,
    ratecode_name STRING
);

CREATE OR REPLACE TABLE `nycab-insights-project.nycab_dwh.vendor_dim` (
    VendorID INT64 NOT NULL,
    vendor_name STRING
);

CREATE OR REPLACE TABLE `nycab-insights-project.nycab_dwh.fact_trips` (
    VendorID INT64,
    datetime_id INT64,
    passenger_count FLOAT64,
    trip_distance FLOAT64,
    PULocationID INT64,
    DOLocationID INT64,
    payment_type INT64,
    RatecodeID INT64,
    store_and_fwd_flag STRING,
    fare_amount FLOAT64,
    extra FLOAT64,
    mta_tax FLOAT64,
    tip_amount FLOAT64,
    tolls_amount FLOAT64,
    improvement_surcharge FLOAT64,
    total_amount FLOAT64,
    congestion_surcharge FLOAT64,
    Airport_fee FLOAT64
);

-- Add primary key constraints with NOT ENFORCED keyword
ALTER TABLE `nycab-insights-project.nycab_dwh.location_dim`
ADD PRIMARY KEY (LocationID) NOT ENFORCED;

ALTER TABLE `nycab-insights-project.nycab_dwh.datetime_dim`
ADD PRIMARY KEY (datetime_id) NOT ENFORCED;

ALTER TABLE `nycab-insights-project.nycab_dwh.payment_type_dim`
ADD PRIMARY KEY (payment_type) NOT ENFORCED;

ALTER TABLE `nycab-insights-project.nycab_dwh.Ratecode_dim`
ADD PRIMARY KEY (RatecodeID) NOT ENFORCED;

ALTER TABLE `nycab-insights-project.nycab_dwh.vendor_dim`
ADD PRIMARY KEY (VendorID) NOT ENFORCED;

ALTER TABLE `nycab-insights-project.nycab_dwh.fact_trips`
ADD PRIMARY KEY (VendorID, datetime_id, PULocationID, DOLocationID, RatecodeID, payment_type) NOT ENFORCED;