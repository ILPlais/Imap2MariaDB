SELECT emails.sender_name,
       emails.sender_address,
       emails.date_sent,
       emails.subject,
       SUM(attachments.size) AS TotalSize,
       folders.full_path
FROM emails
    JOIN attachments ON (attachments.email_id = emails.id)
    JOIN folders     ON (folders.id = emails.folder_id)
GROUP BY attachments.email_id
ORDER BY SUM(attachments.size) DESC
LIMIT 10;
/*********************************************************************************************************************/
/* Display the totals in MiB */
SELECT emails.sender_name,
       emails.sender_address,
       emails.date_sent,
       emails.subject,
       ROUND((SUM(attachments.size) / 1024 / 1024), 1) AS TotalSizeMiB,
       folders.full_path
FROM emails
    JOIN attachments ON (attachments.email_id = emails.id)
    JOIN folders     ON (folders.id = emails.folder_id)
GROUP BY attachments.email_id
ORDER BY SUM(attachments.size) DESC
LIMIT 10;