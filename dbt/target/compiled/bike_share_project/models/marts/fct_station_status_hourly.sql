

with base as (

    select
        cast(station_id as varchar)         as station_id,
        date_trunc('hour', fetched_at)      as hour_ts,
        avg(num_bikes_available)            as avg_bikes_available,
        avg(num_docks_available)            as avg_docks_available,
        max(last_reported)                  as last_reported_ts
    from BIKE_SHARE_DB.BIKE_SHARE_RAW_DATA.RAW_STATION_STATUS
    
      where fetched_at > (
          select coalesce(max(hour_ts), '1900-01-01'::timestamp)
          from BIKE_SHARE_DB.BIKE_SHARE_ANALYTICS.fct_station_status_hourly
      )
    
    group by 1,2
)

select
    station_id,
    hour_ts,
    avg_bikes_available,
    avg_docks_available,
    last_reported_ts,
    md5(station_id || '|' || to_char(hour_ts, 'YYYY-MM-DD HH24:00:00')) as station_id_hour_ts
from base