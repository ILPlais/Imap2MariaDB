#!/usr/bin/env python3
"""
Imap2MariaDB - Exports all emails from an IMAP account to a MariaDB database.

Stores raw sources, metadata (Message-ID, subject, sender, recipients),
the list of attachments with their type, as well as message bodies in plain
text and HTML.
"""

import argparse
import base64
import configparser
import csv
import email
from typing import Any
import email.header
import email.utils
import imaplib
import logging
import re
import ssl
import sys
import time
from datetime import datetime, timezone
from email.message import Message

import chardet
import mysql.connector
from mysql.connector import Error as MySQLError
from tqdm import tqdm

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
SQL_CREATE_FOLDERS = """
CREATE TABLE IF NOT EXISTS folders (
    id              BIGINT        UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name            VARCHAR(255)  NOT NULL,
    full_path       VARCHAR(1024) NOT NULL,
    parent_id       BIGINT        UNSIGNED NULL,
    `delimiter`     VARCHAR(10)   NULL,
    UNIQUE KEY      uq_full_path (full_path),
    CONSTRAINT      fk_folders_parent FOREIGN KEY (parent_id)
        REFERENCES folders(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

SQL_CREATE_EMAILS = """
CREATE TABLE IF NOT EXISTS emails (
    id              BIGINT       UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    message_id      VARCHAR(512) NULL,
    folder_id       BIGINT       UNSIGNED NOT NULL,
    subject         TEXT         NULL,
    sender_name     VARCHAR(512) NULL,
    sender_address  VARCHAR(512) NULL,
    date_sent       DATETIME     NULL,
    in_reply_to     VARCHAR(512) NULL,
    body_text       LONGTEXT     NULL,
    body_html       LONGTEXT     NULL,
    raw_source      LONGBLOB     NOT NULL,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY      uq_message_id_folder (message_id, folder_id),
    INDEX           idx_in_reply_to (in_reply_to),
    CONSTRAINT fk_emails_folder FOREIGN KEY (folder_id)
        REFERENCES folders(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

SQL_CREATE_EMAIL_REFERENCES = """
CREATE TABLE IF NOT EXISTS email_references (
    id                    BIGINT       UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    email_id              BIGINT       UNSIGNED NOT NULL,
    referenced_message_id VARCHAR(512) NOT NULL,
    position              INT          UNSIGNED NOT NULL,
    CONSTRAINT fk_emailref_email FOREIGN KEY (email_id)
        REFERENCES emails(id) ON DELETE CASCADE,
    INDEX idx_referenced_message_id (referenced_message_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

SQL_CREATE_RECIPIENTS = """
CREATE TABLE IF NOT EXISTS recipients (
    id              BIGINT       UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    email_id        BIGINT       UNSIGNED NOT NULL,
    type            ENUM('From','To','Cc','Bcc','Reply-To') NOT NULL,
    name            VARCHAR(512) NULL,
    address         VARCHAR(512) NULL,
    CONSTRAINT fk_recipients_email FOREIGN KEY (email_id)
        REFERENCES emails(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

SQL_CREATE_EMAIL_HEADERS = """
CREATE TABLE IF NOT EXISTS headers (
    id              BIGINT      UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    email_id        BIGINT      UNSIGNED NOT NULL,
    field_name      VARCHAR(512) NOT NULL,
    field_value     TEXT        NULL,
    CONSTRAINT fk_headers_email FOREIGN KEY (email_id)
        REFERENCES emails(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

SQL_CREATE_ATTACHMENTS = """
CREATE TABLE IF NOT EXISTS attachments (
    id              BIGINT       UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    email_id        BIGINT       UNSIGNED NOT NULL,
    filename        TEXT         NULL,
    content_type    VARCHAR(255) NULL,
    size            BIGINT       UNSIGNED NULL,
    CONSTRAINT fk_attachments_email FOREIGN KEY (email_id)
        REFERENCES emails(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

SQL_GET_OR_CREATE_FOLDER = """
INSERT IGNORE INTO folders (name, full_path, parent_id, `delimiter`)
VALUES (%s, %s, %s, %s)
"""

SQL_GET_FOLDER_ID = """
SELECT id FROM folders WHERE full_path = %s LIMIT 1
"""

SQL_INSERT_EMAIL = """
INSERT INTO emails
    (message_id, folder_id, subject, sender_name, sender_address, date_sent,
     in_reply_to, body_text, body_html, raw_source)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

SQL_INSERT_EMAIL_REFERENCE = """
INSERT INTO email_references (email_id, referenced_message_id, position)
VALUES (%s, %s, %s)
"""

SQL_INSERT_RECIPIENT = """
INSERT INTO recipients (email_id, type, name, address)
VALUES (%s, %s, %s, %s)
"""

SQL_INSERT_HEADER = """
INSERT INTO headers (email_id, field_name, field_value)
VALUES (%s, %s, %s)
"""

SQL_INSERT_ATTACHMENT = """
INSERT INTO attachments (email_id, filename, content_type, size)
VALUES (%s, %s, %s, %s)
"""

SQL_CHECK_EXISTS = """
SELECT 1 FROM emails WHERE message_id = %s AND folder_id = %s LIMIT 1
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


def parse_message_id(raw: str | None) -> str | None:
    """Extracts a single Message-ID, stripping angle brackets and whitespace."""
    if not raw:
        return None
    raw = raw.strip().strip("<>").strip()
    return raw if raw else None


def decode_imap_utf7(s: str) -> str:
    """Decodes IMAP modified UTF-7 (RFC 3501) into Unicode.

    Converts encoded names like '&2Dzfpw- Deezer' to '🎧 Deezer'.
    Returns the original string on decode errors.
    """
    if not s or "&" not in s:
        return s
    try:

        def b64padanddecode(b: str) -> str:
            b += (-len(b) % 4) * "="
            return base64.b64decode(b, altchars=b"+,", validate=True).decode("utf-16-be")

        parts = s.split("&")
        out = parts[0]
        for e in parts[1:]:
            u, a = (e.split("-", 1) if "-" in e else (e, ""))
            if u == "":
                out += "&"
            else:
                out += b64padanddecode(u)
            out += a
        return out
    except Exception:
        return s


def parse_message_ids(raw: str | None) -> list[str]:
    """Extracts a list of Message-IDs from a References-style header.

    The References header contains space-separated Message-IDs enclosed
    in angle brackets, e.g.::

        <id1@host> <id2@host> <id3@host>
    """
    if not raw:
        return []
    ids: list[str] = []
    for match in re.finditer(r"<([^>]+)>", raw):
        mid = match.group(1).strip()
        if mid:
            ids.append(mid)
    # Fallback: if no angle brackets found, try whitespace split
    if not ids:
        for token in raw.split():
            token = token.strip("<>").strip()
            if token:
                ids.append(token)
    return ids


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

# MySQL/MariaDB error codes that indicate a transient problem worth retrying.
TRANSIENT_MYSQL_ERRNOS = {
    1040,  # ER_CON_COUNT_ERROR   — Too many connections
    1205,  # ER_LOCK_WAIT_TIMEOUT — Lock wait timeout exceeded
    1213,  # ER_LOCK_DEADLOCK     — Deadlock found
    2003,  # CR_CONN_HOST_ERROR   — Can't connect to MySQL server
    2006,  # CR_SERVER_GONE_ERROR — MySQL server has gone away
    2013,  # CR_SERVER_LOST       — Lost connection during query
    2055,  # CR_SERVER_LOST_EXTENDED — Lost connection reading auth packet
}

# Maximum number of retry attempts for transient DB errors.
DB_MAX_RETRIES = 3
# Delay between retries (seconds); doubles after each attempt.
DB_RETRY_BASE_DELAY = 2


def is_transient_db_error(exc: Exception) -> bool:
    """Returns True if *exc* is a transient database error worth retrying."""
    if isinstance(exc, mysql.connector.errors.InterfaceError):
        # InterfaceError covers "MySQL Connection not available" and similar.
        return True
    if isinstance(exc, MySQLError) and getattr(exc, "errno", None) in TRANSIENT_MYSQL_ERRNOS:
        return True
    return False


def format_db_error(exc: Exception) -> str:
    """Returns a detailed one-line description of a database error."""
    parts: list[str] = [type(exc).__name__]
    if hasattr(exc, "errno"):
        parts.append(f"errno={exc.errno}")
    if hasattr(exc, "sqlstate") and exc.sqlstate:
        parts.append(f"sqlstate={exc.sqlstate}")
    parts.append(str(exc).replace("\n", " "))
    return " | ".join(parts)


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
        connection_timeout=30,
    )
    return conn


def log_db_server_info(conn: mysql.connector.MySQLConnection) -> None:
    """Queries and logs key server variables for diagnostics."""
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT @@version, @@connection_id, "
            "@@wait_timeout, @@interactive_timeout, "
            "@@max_allowed_packet, @@net_read_timeout, "
            "@@net_write_timeout"
        )
        row = cursor.fetchone()
        cursor.close()
        if row:
            version, conn_id, wait_t, inter_t, max_pkt, net_r, net_w = row
            log.info(
                "DB server: version=%s  connection_id=%s  "
                "wait_timeout=%ss  interactive_timeout=%ss  "
                "max_allowed_packet=%s  "
                "net_read_timeout=%ss  net_write_timeout=%ss",
                version, conn_id, wait_t, inter_t,
                f"{int(max_pkt) / (1024 * 1024):.0f}MB" if max_pkt else "?",
                net_r, net_w,
            )
    except Exception as exc:
        log.warning("Could not query DB server variables: %s", exc)


def ensure_db_connection(
    conn: mysql.connector.MySQLConnection,
    db_cfg: configparser.SectionProxy,
) -> mysql.connector.MySQLConnection:
    """Ensures the DB connection is alive; reconnects if needed.

    Returns the existing connection if healthy, or a brand-new one
    if the original was lost.
    """
    try:
        conn.ping(reconnect=True, attempts=2, delay=1)
        return conn
    except Exception as exc:
        log.warning(
            "DB connection lost (ping failed: %s). Reconnecting…",
            format_db_error(exc),
        )
    # ping failed even after internal reconnect — open a fresh connection
    try:
        conn.close()
    except Exception:
        pass
    new_conn = get_db_connection(db_cfg)
    log.info("DB reconnection successful (new connection_id).")
    log_db_server_info(new_conn)
    return new_conn


def init_database(conn: mysql.connector.MySQLConnection) -> None:
    """Creates tables if they do not exist."""
    cursor = conn.cursor()
    cursor.execute(SQL_CREATE_FOLDERS)
    cursor.execute(SQL_CREATE_EMAILS)
    cursor.execute(SQL_CREATE_EMAIL_REFERENCES)
    cursor.execute(SQL_CREATE_RECIPIENTS)
    cursor.execute(SQL_CREATE_EMAIL_HEADERS)
    cursor.execute(SQL_CREATE_ATTACHMENTS)
    conn.commit()
    cursor.close()
    log.info("Database schema verified / created.")


def get_or_create_folder(
    conn: mysql.connector.MySQLConnection,
    decoded_path: str,
    delimiter: str | None,
    folder_cache: dict[str, int],
) -> int:
    """Returns the folder id, creating the full folder hierarchy if needed.

    Uses decoded_path (Unicode) for DB storage so names like '🎧 Deezer'
    are stored instead of '&2Dzfpw- Deezer'.

    For example, given ``decoded_path="INBOX/🎧 Deezer"`` and ``delimiter="/"``,
    the function ensures that rows for ``INBOX`` and ``INBOX/🎧 Deezer``
    exist, with correct ``parent_id`` links.
    """
    if decoded_path in folder_cache:
        return folder_cache[decoded_path]

    cursor = conn.cursor()
    try:
        # Determine the parts of the hierarchy
        if delimiter and delimiter in decoded_path:
            parts = decoded_path.split(delimiter)
        else:
            parts = [decoded_path]

        parent_id: int | None = None
        accumulated_path = ""

        for i, part in enumerate(parts):
            if accumulated_path:
                accumulated_path += delimiter + part
            else:
                accumulated_path = part

            # Fast path: already cached
            if accumulated_path in folder_cache:
                parent_id = folder_cache[accumulated_path]
                continue

            # Check if it already exists in DB
            cursor.execute(SQL_GET_FOLDER_ID, (accumulated_path,))
            row = cursor.fetchone()
            if row:
                folder_id = row[0]
            else:
                cursor.execute(SQL_GET_OR_CREATE_FOLDER, (
                    part, accumulated_path, parent_id, delimiter,
                ))
                conn.commit()
                cursor.execute(SQL_GET_FOLDER_ID, (accumulated_path,))
                row = cursor.fetchone()
                folder_id = row[0]

            folder_cache[accumulated_path] = folder_id
            parent_id = folder_id

        return folder_cache[decoded_path]
    finally:
        cursor.close()


def email_exists(cursor, message_id: str | None, folder_id: int) -> bool:
    """Checks if an email with this Message-ID already exists in this folder."""
    if not message_id:
        return False
    cursor.execute(SQL_CHECK_EXISTS, (message_id, folder_id))
    return cursor.fetchone() is not None


def _do_insert_email(
    conn: mysql.connector.MySQLConnection,
    folder_id: int,
    raw_bytes: bytes,
    msg: Message,
    skip_existing: bool,
) -> tuple[bool, str | None, datetime | None, str | None, str | None]:
    """Core insertion logic (no retry). Called by :func:`insert_email`."""
    cursor = conn.cursor()
    try:
        message_id = msg.get("Message-ID", "").strip() or None
        if message_id:
            message_id = message_id.strip("<>")

        if skip_existing and email_exists(cursor, message_id, folder_id):
            return (False, None, None, None, None)

        subject = decode_header_value(msg.get("Subject"))
        sender = parse_addresses(msg.get("From"))
        sender_name = sender[0][0] if sender else None
        sender_address = sender[0][1] if sender else None
        date_sent = parse_date(msg.get("Date"))

        in_reply_to = parse_message_id(msg.get("In-Reply-To"))
        references = parse_message_ids(msg.get("References"))

        body_text, body_html = extract_bodies(msg)
        attachments = extract_attachments(msg)

        raw_size = len(raw_bytes)
        log.debug(
            "Inserting email: Message-ID=%s  folder_id=%d  raw_size=%d bytes  "
            "subject=%r  date=%s",
            message_id, folder_id, raw_size,
            (subject or "")[:80], date_sent,
        )

        # Insert email
        cursor.execute(SQL_INSERT_EMAIL, (
            message_id,
            folder_id,
            subject or None,
            sender_name or None,
            sender_address or None,
            date_sent,
            in_reply_to,
            body_text or None,
            body_html or None,
            raw_bytes,
        ))
        email_id = cursor.lastrowid

        # Insert references (threading chain)
        for pos, ref_mid in enumerate(references):
            cursor.execute(SQL_INSERT_EMAIL_REFERENCE, (
                email_id, ref_mid, pos,
            ))

        # Insert recipients
        for header_type in ("From", "To", "Cc", "Bcc", "Reply-To"):
            for name, address in parse_addresses(msg.get(header_type)):
                cursor.execute(SQL_INSERT_RECIPIENT, (
                    email_id, header_type, name or None, address or None,
                ))

        # Insert extra headers (those not already stored in dedicated columns)
        handled_headers = {
            "message-id", "subject", "from", "to", "cc", "bcc",
            "reply-to", "date", "in-reply-to", "references",
        }
        for field_name, field_value in msg.items():
            if field_name.lower() in handled_headers:
                continue
            decoded_value = decode_header_value(field_value)
            cursor.execute(SQL_INSERT_HEADER, (
                email_id, field_name, decoded_value or None,
            ))

        # Insert attachments
        for filename, content_type, size in attachments:
            cursor.execute(SQL_INSERT_ATTACHMENT, (
                email_id, filename, content_type, size,
            ))

        conn.commit()
        return (True, message_id, date_sent, sender_address, subject or None)

    except MySQLError as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        # Duplicate: message already present (unique constraint)
        if getattr(exc, "errno", None) == 1062:
            log.debug("Duplicate ignored: Message-ID=%s folder_id=%d",
                      msg.get("Message-ID", "?"), folder_id)
            return (False, None, None, None, None)
        raise
    finally:
        cursor.close()


def insert_email(
    conn: mysql.connector.MySQLConnection,
    db_cfg: configparser.SectionProxy,
    folder_id: int,
    raw_bytes: bytes,
    msg: Message,
    skip_existing: bool,
) -> tuple[mysql.connector.MySQLConnection, bool, str | None, datetime | None, str | None, str | None]:
    """Inserts an email with automatic retry on transient DB errors.

    Returns ``(conn, inserted, message_id, date_sent, sender_address, subject)``.
    The returned *conn* may differ from the input if a reconnection occurred.
    """
    last_exc: Exception | None = None
    for attempt in range(1, DB_MAX_RETRIES + 1):
        try:
            result = _do_insert_email(conn, folder_id, raw_bytes, msg, skip_existing)
            return (conn,) + result
        except Exception as exc:
            last_exc = exc
            msg_id = msg.get("Message-ID", "?")
            if is_transient_db_error(exc):
                delay = DB_RETRY_BASE_DELAY * (2 ** (attempt - 1))
                log.warning(
                    "Transient DB error on attempt %d/%d for Message-ID=%s: %s  "
                    "— retrying in %ds…",
                    attempt, DB_MAX_RETRIES, msg_id,
                    format_db_error(exc), delay,
                )
                time.sleep(delay)
                # Try to restore the connection before next attempt
                conn = ensure_db_connection(conn, db_cfg)
            else:
                # Non-transient error: log and raise immediately
                log.error(
                    "Non-transient DB error inserting Message-ID=%s folder_id=%d: %s",
                    msg_id, folder_id, format_db_error(exc),
                )
                raise

    # All retries exhausted
    log.error(
        "All %d retry attempts failed for Message-ID=%s folder_id=%d. Last error: %s",
        DB_MAX_RETRIES, msg.get("Message-ID", "?"), folder_id,
        format_db_error(last_exc) if last_exc else "unknown",
    )
    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# IMAP
# ---------------------------------------------------------------------------

def connect_imap(config: configparser.SectionProxy) -> imaplib.IMAP4 | imaplib.IMAP4_SSL:
    """Establishes an IMAP connection."""
    host = config.get("host")
    port = config.getint("port", 993)
    use_ssl = config.getboolean("ssl", True)

    if use_ssl:
        # Python 3.10+ prohibits RSA encryption by default.
        # Some IMAP servers (particularly legacy hosts) still require it.
        # Using DEFAULT restores compatibility with these servers.
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT")
        imap = imaplib.IMAP4_SSL(host, port, ssl_context=ctx)
    else:
        imap = imaplib.IMAP4(host, port)

    user = config.get("user")
    password = config.get("password")
    imap.login(user, password)
    log.info("Connected to IMAP server %s as %s", host, user)
    return imap


def parse_imap_list_response(raw_line: bytes | str) -> tuple[str, str | None]:
    """Parses an IMAP LIST response line.

    Returns ``(folder_name, delimiter)``.  The delimiter is ``None``
    when the server indicates NIL.

    Example input::

        (\\HasNoChildren) "/" "INBOX/Subfolder"
        (\\HasNoChildren) "." INBOX.Subfolder
    """
    if isinstance(raw_line, bytes):
        raw_line = raw_line.decode("utf-8", errors="replace")

    # --- extract flags part between ( and ) ---
    idx = raw_line.find(") ")
    if idx == -1:
        return raw_line.split()[-1].strip('"'), None

    rest = raw_line[idx + 2:].strip()

    # --- extract delimiter ---
    delimiter: str | None = None
    if rest.startswith('"'):
        end_delim = rest.index('"', 1)
        delimiter = rest[1:end_delim]
        rest = rest[end_delim + 1:].strip()
    elif rest.upper().startswith("NIL"):
        rest = rest[3:].strip()
    else:
        # Non-quoted single character delimiter
        delimiter = rest[0]
        rest = rest[1:].strip()

    folder_name = rest.strip('"')
    return folder_name, delimiter


def get_folders(
    imap: imaplib.IMAP4, config_folders: str,
) -> list[tuple[str, str, str | None]]:
    """Returns the list of ``(encoded_path, decoded_path, delimiter)`` to process.

    encoded_path: used for IMAP SELECT/operations (server expects UTF-7 encoded).
    decoded_path: used for DB storage and display (Unicode e.g. 🎧 Deezer).
    """
    status, data = imap.list()
    if status != "OK":
        log.error("Unable to list IMAP folders.")
        return []

    # Build a map of all folders with their delimiter
    all_folders: list[tuple[str, str, str | None]] = []
    for item in data:
        if item is None:
            continue
        # imaplib may return (bytes, bytes) tuples when the server sends
        # folder names as IMAP literals (e.g. {5}\r\nINBOX).  The first
        # element contains the flags, delimiter and a literal size marker
        # {n}; the second element is the raw folder name.  We rebuild a
        # standard LIST response line so parse_imap_list_response can
        # handle it.
        if isinstance(item, tuple):
            prefix = item[0] if isinstance(item[0], bytes) else str(item[0]).encode()
            literal = item[1] if isinstance(item[1], bytes) else str(item[1]).encode()
            # Strip the literal size marker {n} from the end of the prefix
            brace_idx = prefix.rfind(b"{")
            if brace_idx != -1:
                prefix = prefix[:brace_idx]
            # Wrap the literal folder name in quotes to form a valid line
            item = prefix + b'"' + literal + b'"'
        try:
            name_encoded, delim = parse_imap_list_response(item)
        except Exception as exc:
            log.warning("Failed to parse IMAP LIST response %r: %s", item, exc)
            continue
        if name_encoded:
            name_decoded = decode_imap_utf7(name_encoded)
            log.debug("  Found folder: %s (delimiter=%r)", name_decoded, delim)
            all_folders.append((name_encoded, name_decoded, delim))

    log.debug("Listed %d folder(s) from IMAP server.", len(all_folders))

    # If specific folders are configured, filter the list
    if config_folders.strip():
        requested = [f.strip() for f in config_folders.split(",") if f.strip()]
        result: list[tuple[str, str, str | None]] = []
        for req in requested:
            for enc, dec, delim in all_folders:
                if enc == req or dec == req:
                    result.append((enc, dec, delim))
                    break
            else:
                # Requested folder not in list; use as-is (may be encoded or decoded)
                dec = decode_imap_utf7(req)
                result.append((req, dec, None))
        return result

    return all_folders


def count_folder_messages(
    imap: imaplib.IMAP4,
    folder: str,
) -> int:
    """Selects an IMAP folder and returns its message count."""
    try:
        status, _ = imap.select(f'"{folder}"', readonly=True)
    except imaplib.IMAP4.error:
        return 0
    if status != "OK":
        return 0
    status, data = imap.search(None, "ALL")
    if status != "OK" or not data[0]:
        return 0
    return len(data[0].split())


def fetch_emails_from_folder(
    imap: imaplib.IMAP4,
    conn: mysql.connector.MySQLConnection,
    db_cfg: configparser.SectionProxy,
    encoded_path: str,
    folder_id: int,
    batch_size: int,
    skip_existing: bool,
    pbar_total: tqdm,
    display_name: str | None = None,
    csv_writer: Any = None,
    csv_file: Any = None,
    full_path: str | None = None,
) -> tuple[mysql.connector.MySQLConnection, int, int]:
    """Fetches and inserts all emails from an IMAP folder.

    Returns ``(conn, inserted_count, total_count)``.
    The returned *conn* may differ from the input if a reconnection occurred.
    """
    display = display_name or encoded_path
    try:
        status, _ = imap.select(f'"{encoded_path}"', readonly=True)
    except imaplib.IMAP4.error as exc:
        tqdm.write(f"WARNING: Unable to select folder '{display}': {exc}")
        return conn, 0, 0

    if status != "OK":
        tqdm.write(f"WARNING: Unable to select folder '{display}'.")
        return conn, 0, 0

    status, data = imap.search(None, "ALL")
    if status != "OK" or not data[0]:
        return conn, 0, 0

    msg_nums = data[0].split()
    total = len(msg_nums)
    inserted = 0
    errors_in_folder = 0

    with tqdm(
        total=total,
        desc=display,
        unit="msg",
        position=1,
        leave=False,
    ) as pbar_folder:
        for i in range(0, total, batch_size):
            batch = msg_nums[i : i + batch_size]
            msg_set = b",".join(batch)

            # Ensure DB connection is alive before each batch
            try:
                conn = ensure_db_connection(conn, db_cfg)
            except Exception as exc:
                tqdm.write(
                    f"ERROR: DB reconnection failed before batch {i} "
                    f"in '{display}': {format_db_error(exc)}"
                )
                pbar_folder.update(len(batch))
                pbar_total.update(len(batch))
                errors_in_folder += len(batch)
                continue

            status, messages_data = imap.fetch(msg_set, "(RFC822)")
            if status != "OK":
                tqdm.write(
                    f"WARNING: Error fetching from '{display}' (batch {i})."
                )
                pbar_folder.update(len(batch))
                pbar_total.update(len(batch))
                continue

            batch_processed = 0
            for response_part in messages_data:
                if not isinstance(response_part, tuple):
                    continue
                raw_email = response_part[1]
                if not isinstance(raw_email, bytes):
                    continue

                try:
                    msg = email.message_from_bytes(raw_email)
                    conn, inserted_flag, mid, dt, sender_addr, subj = insert_email(
                        conn, db_cfg, folder_id, raw_email, msg, skip_existing
                    )
                    if inserted_flag:
                        inserted += 1
                        if csv_writer is not None and full_path is not None:
                            csv_writer.writerow([
                                mid or "",
                                dt.isoformat() if dt else "",
                                sender_addr or "",
                                subj or "",
                                full_path,
                            ])
                            if csv_file is not None:
                                csv_file.flush()
                except Exception as exc:
                    errors_in_folder += 1
                    tqdm.write(
                        f"ERROR: Failed to process message in '{display}': "
                        f"{format_db_error(exc) if isinstance(exc, (MySQLError, mysql.connector.errors.InterfaceError)) else exc}"
                    )

                batch_processed += 1
                pbar_folder.update(1)
                pbar_total.update(1)

            pbar_folder.set_postfix(inserted=inserted, errors=errors_in_folder)

    if errors_in_folder:
        tqdm.write(
            f"WARNING: Folder '{display}' completed with {errors_in_folder} error(s)."
        )

    return conn, inserted, total


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

    # Load configuration (try UTF-8, then Latin-1 for files from Windows/legacy systems)
    config = configparser.ConfigParser()
    try:
        with open(args.config, encoding="utf-8") as f:
            config.read_file(f)
    except UnicodeDecodeError:
        try:
            with open(args.config, encoding="latin-1") as f:
                config.read_file(f)
            log.debug("Configuration file read with Latin-1 encoding.")
        except OSError:
            log.error("Unable to read configuration file: %s", args.config)
            sys.exit(1)
    except OSError as e:
        log.error("Unable to read configuration file: %s: %s", args.config, e)
        sys.exit(1)

    imap_cfg = config["imap"]
    db_cfg = config["database"]
    options = config["options"] if config.has_section("options") else {}

    batch_size = int(options.get("batch_size", 100))
    skip_existing = str(options.get("skip_existing", "true")).lower() in ("true", "1", "yes")
    csv_log_path = (options.get("csv_log") or "").strip()

    # Connect to database
    try:
        db_conn = get_db_connection(db_cfg)
        log_db_server_info(db_conn)
        init_database(db_conn)
    except MySQLError as exc:
        log.error("Database connection error: %s", format_db_error(exc))
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

    log.info("Folders to process: %s", ", ".join(dec for _, dec, _ in folders))

    # Cache for folder path -> folder id mapping (decoded paths)
    folder_cache: dict[str, int] = {}

    # Phase 1: count messages per folder and resolve folder ids
    folder_infos: list[tuple[str, str, str | None, int, int]] = []
    log.info("Counting messages per folder...")
    for encoded_path, decoded_path, delimiter in folders:
        folder_id = get_or_create_folder(
            db_conn, decoded_path, delimiter, folder_cache,
        )
        msg_count = count_folder_messages(imap, encoded_path)
        folder_infos.append((encoded_path, decoded_path, delimiter, folder_id, msg_count))

    total_messages = sum(c for _, _, _, _, c in folder_infos)
    log.info("Total: %d message(s) across %d folder(s).",
             total_messages, len(folder_infos))

    # Optional CSV log of processed emails
    csv_file = None
    csv_writer = None
    if csv_log_path:
        csv_file = open(csv_log_path, "a", newline="", encoding="utf-8")
        if csv_file.tell() == 0:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([
                "message_id", "date_sent", "sender_address", "subject", "full_path"
            ])
        else:
            csv_writer = csv.writer(csv_file)
        log.info("Logging inserted emails to CSV: %s", csv_log_path)

    # Phase 2: process with progress bars
    total_inserted = 0

    with tqdm(
        total=total_messages,
        desc="Total",
        unit="msg",
        position=0,
    ) as pbar_total:
        for encoded_path, decoded_path, delimiter, folder_id, msg_count in folder_infos:
            if msg_count == 0:
                continue
            db_conn, inserted, count = fetch_emails_from_folder(
                imap, db_conn, db_cfg, encoded_path, folder_id,
                batch_size, skip_existing, pbar_total,
                display_name=decoded_path,
                csv_writer=csv_writer,
                csv_file=csv_file,
                full_path=decoded_path,
            )
            total_inserted += inserted
            pbar_total.set_postfix(inserted=total_inserted)

    # Cleanup
    if csv_file is not None:
        csv_file.close()
    try:
        imap.logout()
    except Exception:
        pass
    try:
        db_conn.close()
    except Exception:
        pass

    log.info("Done. %d message(s) inserted out of %d processed.",
             total_inserted, total_messages)


if __name__ == "__main__":
    main()
