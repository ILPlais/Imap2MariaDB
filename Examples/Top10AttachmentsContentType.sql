SELECT attachments.content_type, COUNT(*)
FROM attachments
GROUP BY attachments.content_type
ORDER BY COUNT(*) DESC
LIMIT 10;