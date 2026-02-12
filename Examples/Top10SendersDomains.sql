SELECT SUBSTRING(emails.sender_address FROM POSITION('@' IN emails.sender_address)) AS Domain, COUNT(*)
FROM emails
GROUP BY Domain
ORDER BY COUNT(*) DESC
LIMIT 10;