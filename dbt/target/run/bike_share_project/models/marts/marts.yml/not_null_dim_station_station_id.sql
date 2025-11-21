select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select station_id
from BIKE_SHARE_DB.BIKE_SHARE_ANALYTICS.dim_station
where station_id is null



      
    ) dbt_internal_test