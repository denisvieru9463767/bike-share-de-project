{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='station_id'
) }}

select
    toString(station_id)              as station_id,
    name,
    short_name,
    region_id,
    lat,
    lon,
    capacity,
    rental_methods,
    station_type,
    external_id,
    eightd_has_key_dispenser,
    electric_bike_surcharge_waiver
from {{ source('bike_share_raw', 'raw_station_info') }}
