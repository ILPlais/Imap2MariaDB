SELECT headers.field_value, COUNT(*)
FROM headers
WHERE headers.field_name = "X-IcodiaSpamFilter"
GROUP BY headers.field_value
ORDER BY COUNT(*) DESC;