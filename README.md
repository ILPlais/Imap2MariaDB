# Imap2MariaDB

Exports all emails from an IMAP account to a MariaDB database.

## Features

- IMAP connection with SSL/TLS support
- Export all folders or select specific folders
- Storage of **raw sources** (RFC822) of each email
- Extraction of **metadata**: Message-ID, subject, sender, sent date
- Extraction of **recipients** (From, To, Cc, Bcc, Reply-To) with names and addresses
- Extraction of **attachment list** with filename, MIME type and size
- Extraction of message **body** in plain text and HTML
- Duplicate detection based on Message-ID (incremental mode)
- Configurable batch processing
- Automatic decoding of MIME headers (RFC 2047) and character encodings

## Prerequisites

- Python 3.10+
- MariaDB or MySQL
- Python dependencies listed in `requirements.txt`

## Installation

```bash
# Clone the repository
git clone https://github.com/votre-utilisateur/Imap2MariaDB.git
cd Imap2MariaDB

# Install dependencies
pip install -r requirements.txt
```

## Configuration

Copy the example configuration file and adapt it:

```bash
cp config.example.ini config.ini
```

Edit `config.ini` with your settings:

```ini
[imap]
host = imap.example.com
port = 993
ssl = true
user = user@example.com
password = your_password
# Folders to export (comma-separated). Empty = all folders.
folders =

[database]
host = localhost
port = 3306
user = root
password = your_db_password
database = imap2mariadb

[options]
batch_size = 100
skip_existing = true
```

### Database creation

The database must exist beforehand. Tables will be created automatically on first run.

```sql
CREATE DATABASE imap2mariadb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

## Usage

```bash
# Standard run
python imap2mariadb.py

# With a custom configuration file
python imap2mariadb.py -c /path/to/config.ini

# Verbose mode
python imap2mariadb.py -v
```

## Database schema

### Table `emails`

| Column        | Type         | Description                         |
|---------------|--------------|-------------------------------------|
| id            | BIGINT       | Auto-increment primary key          |
| message_id    | VARCHAR(512) | Message-ID header                   |
| folder        | VARCHAR(255) | Source IMAP folder                  |
| subject       | TEXT         | Message subject                      |
| sender_name   | VARCHAR(512) | Sender name                          |
| sender_address| VARCHAR(512) | Sender address                       |
| date_sent     | DATETIME     | Sent date (UTC)                      |
| body_text     | LONGTEXT     | Plain text body                      |
| body_html     | LONGTEXT     | HTML body                            |
| raw_source    | LONGBLOB     | Complete raw source (RFC822)         |
| created_at    | DATETIME     | Insertion date in database           |

### Table `recipients`

| Column   | Type         | Description                             |
|----------|--------------|-----------------------------------------|
| id       | BIGINT       | Auto-increment primary key              |
| email_id | BIGINT       | Reference to `emails.id`                |
| type     | ENUM         | Type: From, To, Cc, Bcc, Reply-To       |
| name     | VARCHAR(512) | Recipient name                          |
| address  | VARCHAR(512) | Recipient email address                 |

### Table `attachments`

| Column      | Type         | Description                          |
|-------------|--------------|--------------------------------------|
| id          | BIGINT       | Auto-increment primary key           |
| email_id    | BIGINT       | Reference to `emails.id`             |
| filename    | TEXT         | Attached file name                   |
| content_type| VARCHAR(255) | MIME type (e.g. `application/pdf`)   |
| size        | BIGINT       | Size in bytes                        |

## License

See the [LICENSE](LICENSE) file.
