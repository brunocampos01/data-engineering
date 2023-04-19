DROP TABLE IF EXISTS temp;
CREATE TABLE temp (data JSONB);

DROP TABLE IF EXISTS trips;
CREATE TABLE trips (
    id SERIAL,
    vendor_id VARCHAR(3),
    pickup_datetime TIMESTAMP WITH TIME ZONE,
    dropoff_datetime TIMESTAMP WITH TIME ZONE,
    passenger_count SMALLINT,
    trip_distance NUMERIC,
    pickup_longitude NUMERIC,
    pickup_latitude NUMERIC,
    rate_code SMALLINT,
    store_and_fwd_flag TEXT,
    dropoff_longitude NUMERIC,
    dropoff_latitude NUMERIC,
    payment_type VARCHAR(10),
    fare_amount NUMERIC,
    surcharge NUMERIC,
    tip_amount NUMERIC,
    tolls_amount NUMERIC,
    total_amount NUMERIC,
    CONSTRAINT PK_id PRIMARY KEY (id)
);