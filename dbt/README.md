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
└── README.md              # This file
```

## 🏗️ Models

### Staging Layer
- **src_bike_share.yml**: Source definitions pointing to Snowflake raw tables

### Marts Layer
- **dim_station**: Dimension table with static station metadata (capacity, name, location)
- **fct_station_status_hourly**: Incremental fact table with hourly bike availability snapshots
  - Calculates `occupancy_rate`
  - Assigns status buckets: `Critical Empty`, `Normal`, `Critical Full`

## ⚙️ Setup

### 1. Configure dbt Profile

Create or update `~/.dbt/profiles.yml`:

```yaml
bike_share_snowflake:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account.region
      user: your_user
      
      # Option A: Password authentication
      password: your_password
      
      # Option B: Key-pair authentication (recommended for automation)
      # private_key_path: /path/to/your/private_key.p8
      # private_key_passphrase: your_passphrase
      
      role: your_role
      database: BIKE_SHARE
      warehouse: COMPUTE_WH
      schema: ANALYTICS
      threads: 4
```

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
Snowflake Raw Layer          dbt Transformations         Analytics Layer
─────────────────────────────────────────────────────────────────────────
RAW_STATION_INFO      →      dim_station            →    ANALYTICS.DIM_STATION
RAW_STATION_STATUS    →      fct_station_status     →    ANALYTICS.FCT_STATION_STATUS_HOURLY
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
# Test Snowflake connection
dbt debug

# Check for syntax errors
dbt parse
```

### Incremental Model Issues
```bash
# Full refresh if incremental logic is broken
dbt run --full-refresh --select fct_station_status_hourly
```

## 📚 Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Snowflake Setup](https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile)
- [Incremental Models](https://docs.getdbt.com/docs/build/incremental-models)

