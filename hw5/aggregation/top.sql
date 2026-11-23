SELECT movie_id, countIf(event_type = 'VIEW_STARTED') AS views
FROM events FINAL
WHERE toDate(timestamp) = {day:Date} AND movie_id != ''
GROUP BY movie_id
HAVING views > 0
ORDER BY views DESC, movie_id
LIMIT 10
