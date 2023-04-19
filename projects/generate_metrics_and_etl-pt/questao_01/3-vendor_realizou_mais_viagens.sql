-- To get the seasons, I took the column pickup_datetime
WITH seasons AS (
    SELECT
        CASE
            WHEN date_part('month', pickup_datetime) IN (12, 1, 2) THEN 'Verão'
            WHEN date_part('month', pickup_datetime) IN (3, 4, 5) THEN 'Outono'
            WHEN date_part('month', pickup_datetime) IN (6, 7, 8) THEN 'Inverno'
            WHEN date_part('month', pickup_datetime) IN (9, 10, 11) THEN 'Primavera'
        END AS estacao,
        vendor_id AS vendor,
        COUNT(*) as total_viagens
    FROM trips
    GROUP BY estacao, vendor_id
    ORDER BY estacao, total_viagens DESC
),
rank_vendors_by_seasons AS (
  SELECT
    estacao,
    vendor,
    total_viagens,
    DENSE_RANK() OVER (PARTITION BY estacao ORDER BY total_viagens DESC) as rank
  FROM seasons
)

SELECT 
    vendor,    
    total_viagens,
    estacao
FROM rank_vendors_by_seasons
WHERE rank = 1;