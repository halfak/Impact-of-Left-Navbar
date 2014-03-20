SELECT
    user_id,
    user_registration,
    "control" as bucket
FROM staging.dartar_leftnavbar_control
INNER JOIN enwiki.user USING (user_id)
UNION 
SELECT
    user_id,
    user_registration,
    "test" as bucket
FROM staging.dartar_leftnavbar_test
INNER JOIN enwiki.user USING (user_id)
