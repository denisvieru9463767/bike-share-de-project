
    
    

select
    station_id_hour_ts as unique_field,
    count(*) as n_records

from BIKE_SHARE_DB.BIKE_SHARE_ANALYTICS.fct_station_status_hourly
where station_id_hour_ts is not null
group by station_id_hour_ts
having count(*) > 1


