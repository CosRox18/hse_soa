SELECT
    uniqExact(user_id) AS dau,
    avgIfOrNull(progress_seconds, event_type = 'VIEW_FINISHED') AS avg_watch_seconds,
    countIf(event_type = 'VIEW_FINISHED') / nullIf(countIf(event_type = 'VIEW_STARTED'), 0) AS conversion,
    count() AS processed
FROM events FINAL
WHERE toDate(timestamp) = {day:Date}
