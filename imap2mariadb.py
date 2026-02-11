#!/usr/bin/env python3
"""
Imap2MariaDB - Exports all emails from an IMAP account to a MariaDB database.

Stores raw sources, metadata (Message-ID, subject, sender, recipients),
the list of attachments with their type, as well as message bodies in plain
text and HTML.
"""

import argparse
import configparser
import email
import email.header
import email.utils
import imaplib
import logging
import sys
from datetime import datetime, timezone
from email.message import Message

import chardet
import mysql.connector
from mysql.connector import Error as MySQLError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("imap2mariadb")

# ---------------------------------------------------------------------------
# SQL Schema
# ---------------------------------------------------------------------------
SQL_CREATE_EMAILS = """
CREATE TABLE IF NOT EXISTS emails (
    id            BIGINT       UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    message_id    VARCHAR(512) NULL,
    folder        VARCHAR(255) NOT NULL,
    subject       TEXT         NULL,
    sender_name   VARCHAR(512) NULL,
    sender_address VARCHAR(512) NULL,
    date_sent     DATETIME     NULL,
    body_text     LONGTEXT     NULL,
    body_html     LONGTEXT     NULL,
    raw_source    LONGBLOB     NOT NULL,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY    uq_message_id_folder (message_id, folder)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

SQL_CREATE_RECIPIENTS = """
CREATE TABLE IF NOT EXISTS recipients (
    id            BIGINT       UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    email_id      BIGINT       UNSIGNED NOT NULL,
    type          ENUM('From','To','Cc','Bcc','Reply-To') NOT NULL,
    name          VARCHAR(512) NULL,
    address       VARCHAR(512) NULL,
    CONSTRAINT fk_recipients_email FOREIGN KEY (email_id)
        REFERENCES emails(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

SQL_CREATE_ATTACHMENTS = """
CREATE TABLE IF NOT EXISTS attachments (
    id            BIGINT       UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    email_id      BIGINT       UNSIGNED NOT NULL,
    filename      TEXT         NULL,
    content_type  VARCHAR(255) NULL,
    size          BIGINT       UNSIGNED NULL,
    CONSTRAINT fk_attachments_email FOREIGN KEY (email_id)
        REFERENCES emails(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

SQL_INSERT_EMAIL = """
INSERT INTO emails
    (message_id, folder, subject, sender_name, sender_address, date_sent,
     body_text, body_html, raw_source)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

SQL_INSERT_RECIPIENT = """
INSERT INTO recipients (email_id, type, name, address)
VALUES (%s, %s, %s, %s)
"""

SQL_INSERT_ATTACHMENT = """
INSERT INTO attachments (email_id, filename, content_type, size)
VALUES (%s, %s, %s, %s)
"""

SQL_CHECK_EXISTS = """
SELECT 1 FROM emails WHERE message_id = %s AND folder = %s LIMIT 1
"""

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def decode_header_value(value: str | None) -> str:
    """Decodes a MIME-encoded header (RFC 2047) into a Unicode string."""
    if not value:
        return ""
    parts = email.header.decode_header(value)
    decoded_parts: list[str] = []
    for part, charset in parts:
        if isinstance(part, bytes):
            charset = charset or "utf-8"
            try:
                decoded_parts.append(part.decode(charset, errors="replace"))
            except (LookupError, UnicodeDecodeError):
                decoded_parts.append(part.decode("utf-8", errors="replace"))
        else:
            decoded_parts.append(part)
    return " ".join(decoded_parts)


def parse_date(date_str: str | None) -> datetime | None:
    """Converts an email header date into a datetime object."""
    if not date_str:
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(date_str)
        # Convert to UTC if timezone-aware, then make naive for MySQL
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except Exception:
        return None


def parse_addresses(header_value: str | None) -> list[tuple[str, str]]:
    """Extracts (name, address) pairs from an address header."""
    if not header_value:
        return []
    decoded = decode_header_value(header_value)
    addresses = email.utils.getaddresses([decoded])
    return [(name.strip(), addr.strip()) for name, addr in addresses if addr]


def decode_payload(part: Message) -> str:
    """Decodes MIME part content into a Unicode string."""
    payload = part.get_payload(decode=True)
    if payload is None:
        return ""
    charset = part.get_content_charset()
    if charset:
        try:
            return payload.decode(charset, errors="replace")
        except (LookupError, UnicodeDecodeError):
            pass
    # Attempt automatic detection
    detected = chardet.detect(payload)
    detected_charset = detected.get("encoding") or "utf-8"
    try:
        return payload.decode(detected_charset, errors="replace")
    except (LookupError, UnicodeDecodeError):
        return payload.decode("utf-8", errors="replace")


def extract_bodies(msg: Message) -> tuple[str, str]:
    """Extracts the text and HTML bodies from an email message."""
    text_parts: list[str] = []
    html_parts: list[str] = []

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition", ""))
            # Skip attachments
            if "attachment" in disposition.lower():
                continue
            if content_type == "text/plain":
                text_parts.append(decode_payload(part))
            elif content_type == "text/html":
                html_parts.append(decode_payload(part))
    else:
        content_type = msg.get_content_type()
        if content_type == "text/plain":
            text_parts.append(decode_payload(msg))
        elif content_type == "text/html":
            html_parts.append(decode_payload(msg))

    return "\n".join(text_parts), "\n".join(html_parts)


def extract_attachments(msg: Message) -> list[tuple[str | None, str | None, int | None]]:
    """Extracts the list of attachments: (filename, mime_type, size)."""
    attachments: list[tuple[str | None, str | None, int | None]] = []
    if not msg.is_multipart():
        return attachments

    for part in msg.walk():
        disposition = str(part.get("Content-Disposition", ""))
        filename = part.get_filename()
        if filename:
            filename = decode_header_value(filename)

        # An attachment is identified by Content-Disposition: attachment
        # or by the presence of a filename
        if "attachment" in disposition.lower() or filename:
            content_type = part.get_content_type()
            payload = part.get_payload(decode=True)
            size = len(payload) if payload else None
            attachments.append((filename, content_type, size))

    return attachments


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_db_connection(config: configparser.SectionProxy) -> mysql.connector.MySQLConnection:
    """Creates and returns a MariaDB connection."""
    conn = mysql.connector.connect(
        host=config.get("host", "localhost"),
        port=config.getint("port", 3306),
        user=config.get("user", "root"),
        password=config.get("password", ""),
        database=config.get("database", "imap2mariadb"),
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
        autocommit=False,
    )
    return conn


def init_database(conn: mysql.connector.MySQLConnection) -> None:
    """Creates tables if they do not exist."""
    cursor = conn.cursor()
    cursor.execute(SQL_CREATE_EMAILS)
    cursor.execute(SQL_CREATE_RECIPIENTS)
    cursor.execute(SQL_CREATE_ATTACHMENTS)
    conn.commit()
    cursor.close()
    log.info("Database schema verified / created.")


def email_exists(cursor, message_id: str | None, folder: str) -> bool:
    """Checks if an email with this Message-ID already exists in this folder."""
    if not message_id:
        return False
    cursor.execute(SQL_CHECK_EXISTS, (message_id, folder))
    return cursor.fetchone() is not None


def insert_email(
    conn: mysql.connector.MySQLConnection,
    folder: str,
    raw_bytes: bytes,
    msg: Message,
    skip_existing: bool,
) -> bool:
    """Inserts an email and its associated data. Returns True if inserted."""
    cursor = conn.cursor()
    try:
        message_id = msg.get("Message-ID", "").strip() or None
        if message_id:
            # Strip angle brackets
            message_id = message_id.strip("<>")

        if skip_existing and email_exists(cursor, message_id, folder):
            return False

        subject = decode_header_value(msg.get("Subject"))
        sender = parse_addresses(msg.get("From"))
        sender_name = sender[0][0] if sender else None
        sender_address = sender[0][1] if sender else None
        date_sent = parse_date(msg.get("Date"))

        body_text, body_html = extract_bodies(msg)
        attachments = extract_attachments(msg)

        # Insert email
        cursor.execute(SQL_INSERT_EMAIL, (
            message_id,
            folder,
            subject or None,
            sender_name or None,
            sender_address or None,
            date_sent,
            body_text or None,
            body_html or None,
            raw_bytes,
        ))
        email_id = cursor.lastrowid

        # Insert recipients
        for header_type in ("From", "To", "Cc", "Bcc", "Reply-To"):
            for name, address in parse_addresses(msg.get(header_type)):
                cursor.execute(SQL_INSERT_RECIPIENT, (
                    email_id, header_type, name or None, address or None,
                ))

        # Insert attachments
        for filename, content_type, size in attachments:
            cursor.execute(SQL_INSERT_ATTACHMENT, (
                email_id, filename, content_type, size,
            ))

        conn.commit()
        return True

    except MySQLError as exc:
        conn.rollback()
        # Duplicate: message already present (unique constraint)
        if exc.errno == 1062:
            log.debug("Duplicate ignored: Message-ID=%s folder=%s",
                      msg.get("Message-ID", "?"), folder)
            return False
        raise
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# IMAP
# ---------------------------------------------------------------------------

def connect_imap(config: configparser.SectionProxy) -> imaplib.IMAP4 | imaplib.IMAP4_SSL:
    """Establishes an IMAP connection."""
    host = config.get("host")
    port = config.getint("port", 993)
    use_ssl = config.getboolean("ssl", True)

    if use_ssl:
        imap = imaplib.IMAP4_SSL(host, port)
    else:
        imap = imaplib.IMAP4(host, port)

    user = config.get("user")
    password = config.get("password")
    imap.login(user, password)
    log.info("Connected to IMAP server %s as %s", host, user)
    return imap


def list_folders(imap: imaplib.IMAP4) -> list[str]:
    """Lists all available IMAP folders."""
    status, data = imap.list()
    if status != "OK":
        log.error("Unable to list IMAP folders.")
        return []

    folders: list[str] = []
    for item in data:
        if isinstance(item, bytes):
            item = item.decode("utf-8", errors="replace")
        # Format is: (\\Flags) "delimiter" "folder_name"
        # Extract folder name
        parts = item.rsplit('"', 2)
        if len(parts) >= 2:
            folder_name = parts[-1].strip()
            if not folder_name:
                # Try another format: without trailing quotes
                folder_name = parts[-2].strip()
        else:
            folder_name = item.split()[-1].strip('"')

        if folder_name:
            folders.append(folder_name)

    return folders


def parse_imap_folder_name(raw_line: bytes | str) -> str:
    """Extracts folder name from an IMAP LIST response line."""
    if isinstance(raw_line, bytes):
        raw_line = raw_line.decode("utf-8", errors="replace")
    # Format: (\\Flags) "sep" name  or  (\\Flags) "sep" "name with spaces"
    # Look for content after the delimiter
    idx = raw_line.find(') "')
    if idx == -1:
        idx = raw_line.find(") ")
    if idx == -1:
        return raw_line.split()[-1].strip('"')

    rest = raw_line[idx + 2:].strip()
    # Skip the delimiter in quotes: "." or "/"
    if rest.startswith('"'):
        end_delim = rest.index('"', 1)
        rest = rest[end_delim + 1:].strip()

    return rest.strip('"')


def get_folders(imap: imaplib.IMAP4, config_folders: str) -> list[str]:
    """Returns the list of folders to process."""
    if config_folders.strip():
        return [f.strip() for f in config_folders.split(",") if f.strip()]

    status, data = imap.list()
    if status != "OK":
        log.error("Unable to list IMAP folders.")
        return []

    folders: list[str] = []
    for item in data:
        name = parse_imap_folder_name(item)
        if name:
            folders.append(name)
    return folders


def fetch_emails_from_folder(
    imap: imaplib.IMAP4,
    conn: mysql.connector.MySQLConnection,
    folder: str,
    batch_size: int,
    skip_existing: bool,
) -> tuple[int, int]:
    """Fetches and inserts all emails from an IMAP folder.
    Returns (inserted_count, total_count).
    """
    try:
        status, _ = imap.select(f'"{folder}"', readonly=True)
    except imaplib.IMAP4.error as exc:
        log.warning("Unable to select folder '%s': %s", folder, exc)
        return 0, 0

    if status != "OK":
        log.warning("Unable to select folder '%s'.", folder)
        return 0, 0

    status, data = imap.search(None, "ALL")
    if status != "OK" or not data[0]:
        log.info("Folder '%s': no messages.", folder)
        return 0, 0

    msg_nums = data[0].split()
    total = len(msg_nums)
    inserted = 0

    log.info("Folder '%s': %d message(s) to process.", folder, total)

    # Batch processing
    for i in range(0, total, batch_size):
        batch = msg_nums[i : i + batch_size]
        # Build query with UID range
        msg_set = b",".join(batch)

        status, messages_data = imap.fetch(msg_set, "(RFC822)")
        if status != "OK":
            log.warning("Error fetching from '%s' (batch %d).", folder, i)
            continue

        for response_part in messages_data:
            if not isinstance(response_part, tuple):
                continue
            raw_email = response_part[1]
            if not isinstance(raw_email, bytes):
                continue

            try:
                msg = email.message_from_bytes(raw_email)
                if insert_email(conn, folder, raw_email, msg, skip_existing):
                    inserted += 1
            except Exception as exc:
                log.error("Error processing message in '%s': %s",
                          folder, exc)

        processed = min(i + batch_size, total)
        log.info("  Folder '%s': %d/%d processed, %d inserted.",
                 folder, processed, total, inserted)

    return inserted, total


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Exports all emails from an IMAP account to MariaDB.",
    )
    parser.add_argument(
        "-c", "--config",
        default="config.ini",
        help="Path to configuration file (default: config.ini)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose mode (DEBUG).",
    )
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)

    # Load configuration
    config = configparser.ConfigParser()
    if not config.read(args.config):
        log.error("Unable to read configuration file: %s", args.config)
        sys.exit(1)

    imap_cfg = config["imap"]
    db_cfg = config["database"]
    options = config["options"] if config.has_section("options") else {}

    batch_size = int(options.get("batch_size", 100))
    skip_existing = str(options.get("skip_existing", "true")).lower() in ("true", "1", "yes")

    # Connect to database
    try:
        db_conn = get_db_connection(db_cfg)
        init_database(db_conn)
    except MySQLError as exc:
        log.error("Database connection error: %s", exc)
        sys.exit(1)

    # IMAP connection
    try:
        imap = connect_imap(imap_cfg)
    except Exception as exc:
        log.error("IMAP connection error: %s", exc)
        db_conn.close()
        sys.exit(1)

    # Get folders
    folders = get_folders(imap, imap_cfg.get("folders", ""))
    if not folders:
        log.warning("No folders to process.")
        imap.logout()
        db_conn.close()
        sys.exit(0)

    log.info("Folders to process: %s", ", ".join(folders))

    total_inserted = 0
    total_messages = 0

    for folder in folders:
        inserted, count = fetch_emails_from_folder(
            imap, db_conn, folder, batch_size, skip_existing,
        )
        total_inserted += inserted
        total_messages += count

    # Cleanup
    try:
        imap.logout()
    except Exception:
        pass
    db_conn.close()

    log.info("Done. %d message(s) inserted out of %d processed.",
             total_inserted, total_messages)


if __name__ == "__main__":
    main()
