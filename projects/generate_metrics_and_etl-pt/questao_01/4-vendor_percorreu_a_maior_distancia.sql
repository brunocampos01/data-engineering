-- To get the distances traveled, I took the col col trip_distance
WITH seasons AS (
    SELECT
        CASE
            WHEN date_part('month', pickup_datetime) IN (12, 1, 2) THEN 'Verão'
            WHEN date_part('month', pickup_datetime) IN (3, 4, 5) THEN 'Outono'
            WHEN date_part('month', pickup_datetime) IN (6, 7, 8) THEN 'Inverno'
            WHEN date_part('month', pickup_datetime) IN (9, 10, 11) THEN 'Primavera'
        END AS estacao,
        vendor_id AS vendor,
        SUM(trip_distance) as total_distancia
    FROM trips
    GROUP BY estacao, vendor_id
    ORDER BY estacao, total_distancia DESC
),
rank_trip_distance_by_seasons AS (
    SELECT 
      estacao,
      vendor,
      total_distancia,
      DENSE_RANK() OVER (PARTITION BY estacao ORDER BY total_distancia DESC) as rank
  FROM seasons
)
SELECT 
    vendor,
    total_distancia,
    estacao
FROM rank_trip_distance_by_seasons
WHERE rank = 1;