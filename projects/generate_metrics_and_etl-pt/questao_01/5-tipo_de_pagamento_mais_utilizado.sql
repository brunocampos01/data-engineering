WITH seasons AS (
    SELECT
        CASE
            WHEN date_part('month', pickup_datetime) IN (12, 1, 2) THEN 'Verão'
            WHEN date_part('month', pickup_datetime) IN (3, 4, 5) THEN 'Outono'
            WHEN date_part('month', pickup_datetime) IN (6, 7, 8) THEN 'Inverno'
            WHEN date_part('month', pickup_datetime) IN (9, 10, 11) THEN 'Primavera'
        END AS estacao,
        UPPER(payment_type) tipo_pagamento,
        COUNT(*) as total_pagamento
    FROM trips
    GROUP BY estacao, tipo_pagamento
    ORDER BY estacao, total_pagamento DESC
),
rank_payment_type_by_seasons AS (
  SELECT 
    estacao,
    tipo_pagamento,
    DENSE_RANK() OVER (PARTITION BY estacao ORDER BY total_pagamento DESC) as rank
  FROM seasons
)
SELECT 
    estacao,
    tipo_pagamento
FROM rank_payment_type_by_seasons
WHERE rank = 1;