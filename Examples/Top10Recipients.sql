SELECT LOWER(recipients.address), COUNT(*)
FROM recipients
WHERE recipients.type IN ("To", "Cc", "Bcc")
GROUP BY LOWER(recipients.address)
ORDER BY COUNT(*) DESC
LIMIT 10;