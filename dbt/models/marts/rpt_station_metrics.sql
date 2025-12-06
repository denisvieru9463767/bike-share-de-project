{{ config(
    materialized='incremental',
    unique_key='station_id_hour_ts',
    engine='ReplacingMergeTree()',
    order_by='(station_id, hour_ts)'
) }}

with station_status as (
    select
        station_id,
        hour_ts,
        avg_bikes_available,
        avg_docks_available,
        last_reported_ts,
        station_id_hour_ts
    from {{ ref('fct_station_status_hourly') }}
    {% if is_incremental() %}
      where hour_ts > (
          select coalesce(max(hour_ts), toDateTime('1900-01-01 00:00:00'))
          from {{ this }}
      )
    {% endif %}
),

station_dim as (
    select
        station_id,
        name,
        short_name,
        lat,
        lon,
        capacity
    from {{ ref('dim_station') }}
),

joined as (
    select
        -- Station identifiers
        ss.station_id,
        ss.hour_ts,
        ss.station_id_hour_ts,
        
        -- Station metadata
        sd.name                                     as station_name,
        sd.short_name,
        sd.lat,
        sd.lon,
        
        -- Core metrics
        sd.capacity,
        ss.avg_bikes_available                      as available_bikes,
        ss.avg_docks_available                      as available_docks,
        
        -- Occupancy = bikes currently docked (capacity - available docks)
        (sd.capacity - ss.avg_docks_available)      as occupancy,
        
        -- Occupancy Rate = percentage of docks occupied by bikes
        case 
            when sd.capacity > 0 
            then round((sd.capacity - ss.avg_docks_available) / sd.capacity * 100, 2)
            else 0 
        end                                         as occupancy_rate,
        
        ss.last_reported_ts
    from station_status ss
    left join station_dim sd
        on ss.station_id = sd.station_id
),

final as (
    select
        station_id,
        hour_ts,
        station_id_hour_ts,
        station_name,
        short_name,
        lat,
        lon,
        capacity,
        available_bikes,
        available_docks,
        occupancy,
        occupancy_rate,
        last_reported_ts,
        
        -- Status classification based on occupancy rate
        -- Critical Empty: <= 10% occupancy (very few bikes available to rent)
        -- Critical Full: >= 90% occupancy (very few docks available to return)
        -- Normal: 11% - 89% occupancy
        case
            when occupancy_rate <= 10 then 'Critical Empty'
            when occupancy_rate >= 90 then 'Critical Full'
            else 'Normal'
        end                                         as station_status,
        case 
        when station_status = 'Critical Empty' then '#FF0000'
        when station_status = 'Critical Full' then '#1E90FF'
        when station_status = 'Normal' then '#32CD32'
        else '#808080'  -- Default gray
        end                                         as point_color
    from joined
)

select * from final

