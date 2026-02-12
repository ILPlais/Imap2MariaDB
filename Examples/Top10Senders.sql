/* Using table emails */
SELECT LOWER(emails.sender_address), COUNT(*)
FROM emails
GROUP BY LOWER(emails.sender_address)
ORDER BY COUNT(*) DESC
LIMIT 10;
/**********************************************************************************************************************/
/* Using table recipients */
SELECT LOWER(recipients.address), COUNT(*)
FROM recipients
WHERE recipients.type = "From"
GROUP BY LOWER(recipients.address)
ORDER BY COUNT(*) DESC;
/**********************************************************************************************************************/
/* Using both tables */
SELECT LOWER(emails.sender_address), COUNT(*) AS NbEmail
FROM emails
GROUP BY LOWER(emails.sender_address)
UNION DISTINCT
SELECT LOWER(recipients.address), COUNT(*) AS NbEmail
FROM recipients
WHERE recipients.type = "From"
GROUP BY LOWER(recipients.address)
ORDER BY NbEmail DESC
LIMIT 10;