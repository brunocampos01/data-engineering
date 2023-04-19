INSERT INTO trips (
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    pickup_longitude,
    pickup_latitude,
    rate_code,
    store_and_fwd_flag,
    dropoff_longitude,
    dropoff_latitude,
    payment_type,
    fare_amount,
    surcharge,
    tip_amount,
    tolls_amount,
    total_amount
)
(
  SELECT 
    (data->> 'vendor_id')::varchar(3),
    (data->> 'pickup_datetime')::timestamp with time zone,
    (data->> 'dropoff_datetime')::timestamp with time zone,
    (data->> 'passenger_count')::smallint,
    (data->> 'trip_distance')::numeric,
    (data->> 'pickup_longitude')::numeric,
    (data->> 'pickup_latitude')::numeric,
    (data->> 'rate_code')::smallint,
    (data->> 'store_and_fwd_flag')::text,
    (data->> 'dropoff_longitude')::numeric,
    (data->> 'dropoff_latitude')::numeric,
    (data->> 'payment_type')::varchar(10),
    (data->> 'fare_amount')::numeric,
    (data->> 'surcharge')::numeric,
    (data->> 'tip_amount')::numeric,
    (data->> 'tolls_amount')::numeric,
    (data->> 'total_amount')::numeric
  FROM temp
);

DROP TABLE temp;