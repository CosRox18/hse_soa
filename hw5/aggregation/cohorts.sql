WITH
    raw AS (
        SELECT user_id, event_type, toDate(timestamp) AS day
        FROM events FINAL
        WHERE toDate(timestamp) <= {day:Date32}
    ),
    first_views AS (
        SELECT user_id, min(day) AS cohort_date
        FROM raw WHERE event_type = 'VIEW_STARTED'
        GROUP BY user_id
    ),
    sizes AS (
        SELECT cohort_date, count() AS cohort_size
        FROM first_views
        WHERE cohort_date BETWEEN {day:Date32} - 7 AND {day:Date32}
        GROUP BY cohort_date
    ),
    activity AS (SELECT DISTINCT user_id, day FROM raw),
    returned AS (
        SELECT f.cohort_date, dateDiff('day', f.cohort_date, a.day) AS day_number,
            uniqExact(f.user_id) AS users
        FROM first_views f INNER JOIN activity a ON f.user_id = a.user_id
        WHERE f.cohort_date BETWEEN {day:Date32} - 7 AND {day:Date32}
            AND a.day BETWEEN f.cohort_date AND f.cohort_date + 7
        GROUP BY f.cohort_date, day_number
    ),
    grid AS (SELECT cohort_date, cohort_size, arrayJoin(range(8)) AS day_number FROM sizes)
SELECT g.cohort_date, toUInt8(g.day_number),
    if(g.cohort_date + g.day_number > {day:Date32}, NULL,
       coalesce(r.users, 0) / g.cohort_size) AS retention
FROM grid g
LEFT JOIN returned r ON g.cohort_date = r.cohort_date AND g.day_number = r.day_number
ORDER BY g.cohort_date, g.day_number
