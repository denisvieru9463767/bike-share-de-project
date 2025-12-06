# 🔄 dbt - Data Build Tool Configuration

This directory contains the dbt project for transforming raw bike share data into analytics-ready models.

## 📁 Project Structure

```
dbt/
├── models/
│   ├── staging/           # Source definitions
│   │   └── src_bike_share.yml
│   └── marts/             # Business-ready models
│       ├── dim_station.sql
│       ├── fct_station_status_hourly.sql
│       └── marts.yml
├── dbt_project.yml        # Project configuration
├── profiles.yml.template  # Profile template for ClickHouse
└── README.md              # This file
```

## 🏗️ Models

### Staging Layer
- **src_bike_share.yml**: Source definitions pointing to ClickHouse raw tables

### Marts Layer
- **dim_station**: Dimension table with static station metadata (capacity, name, location)
- **fct_station_status_hourly**: Incremental fact table with hourly bike availability snapshots
  - Calculates `occupancy_rate`
  - Assigns status buckets: `Critical Empty`, `Normal`, `Critical Full`

## ⚙️ Setup

### 1. Configure dbt Profile

Copy the template to your dbt config directory:

```bash
cp dbt/profiles.yml.template ~/.dbt/profiles.yml
```

The profile uses environment variables for credentials. Make sure your `.env` file is configured:

```bash
# In your project root
cp .env.example .env
# Edit .env with your credentials
```

Required environment variables:
- `CLICKHOUSE_HOST` - ClickHouse hostname (default: `clickhouse` for Docker)
- `CLICKHOUSE_PORT` - HTTP port (default: `8123`)
- `CLICKHOUSE_USER` - Database user
- `CLICKHOUSE_PASSWORD` - Database password
- `CLICKHOUSE_DATABASE` - Database name

### 2. Test Connection

```bash
cd dbt
dbt debug
```

### 3. Install Dependencies

```bash
dbt deps
```

## 🚀 Running dbt

### Run all models
```bash
dbt run
```

### Run specific model
```bash
dbt run --select dim_station
dbt run --select fct_station_status_hourly
```

### Run tests
```bash
dbt test
```

### Generate documentation
```bash
dbt docs generate
dbt docs serve
```

## 📊 Data Flow

```
ClickHouse Raw Layer         dbt Transformations         Analytics Layer
─────────────────────────────────────────────────────────────────────────
raw_station_info      →      dim_station            →    dim_station
raw_station_status    →      fct_station_status     →    fct_station_status_hourly
```

## 🔍 Key Metrics Calculated

| Metric | Description | Formula |
|--------|-------------|---------|
| `occupancy_rate` | % of docks with bikes | `num_bikes_available / capacity * 100` |
| `status` | Station health status | Based on occupancy thresholds |

### Status Thresholds
- **Critical Empty** (🔴): ≤ 10% occupancy
- **Normal** (🟢): 11% - 89% occupancy  
- **Critical Full** (🔵): ≥ 90% occupancy

## 🐛 Troubleshooting

### Connection Issues
```bash
# Test ClickHouse connection
dbt debug

# Check for syntax errors
dbt parse
```

### Incremental Model Issues
```bash
# Full refresh if incremental logic is broken
dbt run --full-refresh --select fct_station_status_hourly
```

### ClickHouse-Specific Tips

- ClickHouse uses `ReplacingMergeTree()` engine for deduplication
- Use `toStartOfHour()` instead of `date_trunc()`
- Use `toString()` instead of `cast(x as varchar)`
- Use `formatDateTime()` instead of `to_char()`

## 📚 Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt-clickhouse Adapter](https://github.com/ClickHouse/dbt-clickhouse)
- [ClickHouse SQL Reference](https://clickhouse.com/docs/en/sql-reference)
- [Incremental Models](https://docs.getdbt.com/docs/build/incremental-models)
