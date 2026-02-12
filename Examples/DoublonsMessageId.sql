SELECT emails.message_id, COUNT(emails.message_id)
FROM emails
GROUP BY message_id
HAVING COUNT(message_id) > 1
ORDER BY COUNT(message_id) DESC
LIMIT 10;