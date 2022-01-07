CREATE VIEW IF NOT EXISTS staging.senior_dismissalcauses AS
SELECT
  CAST(caudem AS int) AS id,
  CAST(desdem AS string) AS cause
FROM raw.senior_causasdesligamento AS causasdesligamento
ORDER BY id DESC;
