SELECT headers.field_value, COUNT(*)
FROM headers
WHERE headers.field_name = "X-ORG-IN-IP-Address"
GROUP BY headers.field_value
ORDER BY COUNT(*) DESC
LIMIT 10;