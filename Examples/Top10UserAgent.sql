SELECT headers.field_value, COUNT(*)
FROM headers
WHERE headers.field_name = "User-Agent"
GROUP BY headers.field_value
ORDER BY COUNT(*) DESC
LIMIT 10;