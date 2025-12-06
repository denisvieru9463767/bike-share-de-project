{{ config(
    materialized='incremental',
    unique_key='station_id_hour_ts',
    engine='ReplacingMergeTree()',
    order_by='(station_id, hour_ts)'
) }}

with base as (

    select
        toString(station_id)                           as station_id,
        toStartOfHour(fetched_at)                      as hour_ts,
        avg(num_bikes_available)                       as avg_bikes_available,
        avg(num_docks_available)                       as avg_docks_available,
        max(last_reported)                             as last_reported_ts
    from {{ source('bike_share_raw', 'raw_station_status') }}
    {% if is_incremental() %}
      where fetched_at > (
          select coalesce(max(hour_ts), toDateTime('1900-01-01 00:00:00'))
          from {{ this }}
      )
    {% endif %}
    group by 1, 2
)

select
    station_id,
    hour_ts,
    avg_bikes_available,
    avg_docks_available,
    last_reported_ts,
    MD5(concat(station_id, '|', formatDateTime(hour_ts, '%Y-%m-%d %H:00:00'))) as station_id_hour_ts
from base
