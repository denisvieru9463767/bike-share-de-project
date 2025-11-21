select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select station_id_hour_ts
from BIKE_SHARE_DB.BIKE_SHARE_ANALYTICS.fct_station_status_hourly
where station_id_hour_ts is null



      
    ) dbt_internal_test