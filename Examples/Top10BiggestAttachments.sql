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