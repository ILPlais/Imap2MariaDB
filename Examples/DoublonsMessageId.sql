SELECT emails.message_id, COUNT(emails.message_id)
FROM emails
GROUP BY message_id
ORDER BY COUNT(message_id)
LIMIT 10;