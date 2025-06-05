# csv2table

A high-performance Python tool for generating PostgreSQL and Redshift DDL from CSV files with intelligent type inference and seamless data loading capabilities.

## Features

- **Intelligent Type Detection**: Automatically infers SQL types from CSV data (integers, numeric, dates, timestamps, JSON, arrays)
- **Multi-Platform Support**: Works with both PostgreSQL and Amazon Redshift
- **Schema Generation**: Creates optimized `CREATE TABLE` statements with proper quoting and constraints
- **Data Loading**: Generates `COPY` statements for efficient bulk data import
- **Redshift Integration**: Full S3 upload and Redshift COPY command generation with AWS credential management
- **Column Processing**: Smart column name cleaning, deduplication, and case normalization
- **Flexible Input**: Supports gzipped files, custom delimiters, missing headers, and partial type detection

## Installation

```bash
# Direct download
curl -o csv2table https://raw.githubusercontent.com/f0rk/csv2table/master/csv2table
chmod +x csv2table

# Or place in your PATH
curl -o ~/.local/bin/csv2table https://raw.githubusercontent.com/f0rk/csv2table/master/csv2table
chmod +x ~/.local/bin/csv2table
```

### Dependencies
- **Core**: Python 3.7+
- **Redshift**: `pip install botocore` (for S3 operations)

## Quick Start

Given a CSV file `sales.csv`:
```csv
id,product,price,date_sold,metadata
1,Widget A,29.99,2024-01-15,"{""category"": ""electronics""}"
2,Widget B,45.50,2024-01-16,"{""category"": ""home""}"
```

### Basic Usage
```bash
# Generate CREATE TABLE statement
csv2table -f sales.csv

# With intelligent type detection
csv2table -f sales.csv -i -n -p --json

# Clean column names and generate COPY statement
csv2table -f sales.csv -m -l -y
```

### Production-Ready Schema
```bash
# Complete setup with transaction safety
csv2table -f sales.csv \
  --integer --numeric --timestamp --json \
  --mogrify --lower \
  --schema analytics \
  --drop --transaction \
  --copy --backslash
```

## Command Reference

### Core Options
| Flag | Long Form | Description |
|------|-----------|-------------|
| `-f` | `--file` | CSV file path (required) |
| `-t` | `--table` | Override table name |
| `--schema` | | Target schema name |
| `-d` | `--delimiter` | CSV delimiter (default: `,`) |
| `-q` | `--quote` | Quote character (default: `"`) |

### Type Detection
| Flag | Description | Example Output |
|------|-------------|----------------|
| `-i` / `--integer` | Detect integer columns | `integer` / `biginteger` |
| `-n` / `--numeric` | Detect decimal numbers | `numeric` / `numeric(24,8)` (Redshift) |
| `-p` / `--timestamp` | Detect datetime values | `timestamp` / `timestamptz` |
| `--date` | Detect date-only values | `date` |
| `--json` / `--jsonb` | Detect JSON structures | `json` / `jsonb` |
| `--array` | Detect PostgreSQL arrays | `text[]`, `integer[]` |
| `-b` / `--big-integer` | Use `bigint` instead of `int` | `biginteger` |
| `--tz` | Use `timestamptz` for timestamps | `timestamptz` |

### Column Processing
| Flag | Description |
|------|-------------|
| `-m` / `--mogrify` | Clean column names (remove special chars) |
| `-l` / `--lower` | Convert names to lowercase |
| `--fix-duplicates` | Handle duplicate column names with suffixes |
| `--missing-headers` | Generate `col_0`, `col_1`, etc. for headerless files |

### Table Operations
| Flag | Description |
|------|-------------|
| `-x` / `--drop` | Add `DROP TABLE` statement |
| `--truncate` | Add `TRUNCATE TABLE` statement |
| `--temporary` | Create temporary table |
| `--cine` | Use `CREATE TABLE IF NOT EXISTS` |
| `--create-schema` | Generate `CREATE SCHEMA` statement |
| `-z` / `--no-create` | Skip table creation (COPY only) |

### Data Loading
| Flag | Description |
|------|-------------|
| `-y` / `--copy` | Generate `COPY` statement |
| `-s` / `--backslash` | Use `\copy` for psql client |
| `--gzip` | Handle gzipped input files |

### Advanced Features
| Flag | Description |
|------|-------------|
| `-1` / `--transaction` | Wrap in `BEGIN`/`COMMIT` |
| `--keep-going N` | Analyze N rows for type detection |
| `--skip-parsing COLS` | Skip type detection for specified columns |
| `--parse-only COLS` | Only detect types for specified columns |
| `--file-column NAME` | Add column with source filename |
| `--serial-column NAME` | Add auto-increment column |
| `--primary-key COLS` | Define primary key constraint |

### Redshift Integration
| Flag | Description |
|------|-------------|
| `--redshift PATH` | Use Redshift mode with credentials file |
| `--redshift <ENV>` | Use environment variables for credentials |
| `--redshift-bucket BUCKET` | Override S3 bucket |
| `--redshift-upload` | Upload file to S3 before generating COPY |

## Examples

### PostgreSQL Workflows

#### Development: Quick Schema Generation
```bash
csv2table -f data.csv -m -l
```

#### Production: Type-Safe with Transaction
```bash
csv2table -f sales_data.csv \
  --integer --numeric --timestamp \
  --mogrify --lower \
  --schema prod \
  --drop --transaction \
  --copy --backslash
```
Output:
```sql
BEGIN;
DROP TABLE IF EXISTS "prod"."sales_data";
CREATE TABLE "prod"."sales_data" (
    "id" INTEGER,
    "amount" NUMERIC,
    "created_at" TIMESTAMP
);
\COPY "prod"."sales_data"("id", "amount", "created_at") 
FROM '/path/to/sales_data.csv' WITH CSV HEADER DELIMITER ',' QUOTE '"';
COMMIT;
```

#### Complex Data Types
```bash
csv2table -f events.csv -i -n --json --array -m -l
```
Detects:
- `user_ids` as `INTEGER[]`
- `metadata` as `JSON`
- `amounts` as `NUMERIC`

### Redshift Workflows

#### Environment Variables Setup
```bash
export AWS_ACCESS_KEY_ID="your_key"
export AWS_SECRET_ACCESS_KEY="your_secret"
export S3_BUCKET="your-bucket"

csv2table -f large_dataset.csv \
  --redshift "<ENV>" \
  --redshift-upload \
  --integer --numeric \
  --schema warehouse
```

#### Credentials File
Create `~/.aws/csv2table_config`:
```ini
# Redshift configuration
aws_access_key_id = AKIA...
aws_secret_access_key = secret...
s3.bucket = data-warehouse-bucket
```

```bash
csv2table -f sales.csv \
  --redshift ~/.aws/csv2table_config \
  --redshift-upload \
  --schema analytics \
  --integer --numeric --timestamp
```

Output:
```sql
CREATE TABLE "analytics"."sales" (
    "id" INTEGER,
    "amount" NUMERIC(24,8),
    "created_at" TIMESTAMP
);
COPY "analytics"."sales"("id", "amount", "created_at") 
FROM 's3://data-warehouse-bucket/sales.csv' 
WITH CREDENTIALS 'aws_access_key_id=AKIA...;aws_secret_access_key=secret...' 
CSV IGNOREHEADER 1 DELIMITER ',' QUOTE '"' 
DATEFORMAT 'YYYY-MM-DD' TIMEFORMAT 'YYYY-MM-DD HH:MI:SS';
```

### Advanced Use Cases

#### Large File Analysis
```bash
# Analyze first 10,000 rows for performance
csv2table -f huge_file.csv --keep-going 10000 -i -n -p
```

#### Selective Type Detection
```bash
# Only detect types for specific columns
csv2table -f mixed_data.csv --parse-only "amount,date,count" -i -n -p
```

#### ETL Pipeline Integration
```bash
#!/bin/bash
# ETL script example

# Generate schema
csv2table -f daily_extract.csv \
  --integer --numeric --timestamp \
  --schema staging \
  --mogrify --lower \
  > create_staging.sql

# Generate load script  
csv2table -f daily_extract.csv \
  --copy --backslash \
  --schema staging \
  --mogrify --lower \
  --no-create \
  > load_staging.sql

# Execute
psql -f create_staging.sql
psql -f load_staging.sql
```

#### Data Migration
```bash
# From PostgreSQL to Redshift
csv2table -f export.csv -i -n -p --schema migration > postgres.sql
csv2table -f export.csv -i -n -p --schema migration --redshift "<ENV>" > redshift.sql
```

## Type Detection Details

### Date/Time Format Support
- **Dates**: `YYYY-MM-DD`, `YYYY/MM/DD`, `MM/DD/YYYY`, `Mon DD, YYYY`
- **Times**: `HH:MM:SS`, `HH:MM:SS.fff`, `HH:MM AM/PM`
- **Timestamps**: Any combination of above with space or `T` separator

### Numeric Detection
- **Integers**: `123`, `-456`, `0`
- **Decimals**: `123.45`, `-0.001`, `1.23e-4`
- **Currency**: Works with `--numeric` flag

### Array Detection (PostgreSQL)
- Format: `{value1,value2,value3}`
- Homogeneous type detection
- Nested arrays not supported

### JSON Detection
- Objects: `{"key": "value"}`
- Arrays: `[1, 2, 3]`
- Validates JSON syntax

## Performance Tips

1. **Large Files**: Use `--keep-going N` to limit analysis rows
2. **Type Detection**: Only enable needed detectors (`-i -n` vs all flags)
3. **Redshift**: Use `--redshift-upload` for files > 100MB
4. **Memory**: Tool processes files row-by-row (streaming)

## Troubleshooting

### Common Issues

**"File does not exist"**
```bash
# Use absolute paths
csv2table -f /full/path/to/file.csv
```

**"Invalid credentials"**
```bash
# Check AWS configuration
aws s3 ls s3://your-bucket/  # Test access
```

**"Type detection too aggressive"**
```bash
# Skip problematic columns
csv2table -f data.csv --skip-parsing "id,notes" -i -n
```

**"Column name conflicts"**
```bash
# Use duplicate handling
csv2table -f data.csv --fix-duplicates -m -l
```

### Redshift-Specific Issues

**"Bucket not found"**
- Verify bucket name and region
- Check IAM permissions for S3 access

**"Date format errors"**
- Tool auto-detects formats, but verify your data consistency
- Use `--date` vs `--timestamp` appropriately

## Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   CSV Input     │───▶│ Type         │───▶│ SQL Generator   │
│                 │    │ Detector     │    │                 │
└─────────────────┘    └──────────────┘    └─────────────────┘
                               │                       │
                       ┌──────────────┐    ┌─────────────────┐
                       │ Name Cleaner │    │ Redshift        │
                       │              │    │ Manager         │
                       └──────────────┘    └─────────────────┘
```

The tool uses a modular architecture with clear separation of concerns:
- **Type Detection**: Intelligent inference with upgrade paths
- **Name Processing**: Configurable cleaning and normalization  
- **SQL Generation**: Database-specific DDL and DML
- **Redshift Integration**: AWS credential management and S3 operations

## Contributing

This tool follows modern Python best practices:
- Type hints throughout
- Dataclasses for configuration
- Modular class architecture
- Comprehensive error handling

Built for data engineers who need reliable, fast CSV-to-SQL conversion with enterprise features.
