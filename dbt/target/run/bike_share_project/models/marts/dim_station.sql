
  
    

        create or replace transient table BIKE_SHARE_DB.BIKE_SHARE_ANALYTICS.dim_station
         as
        (

select
    cast(station_id as varchar)      as station_id,
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
from BIKE_SHARE_DB.BIKE_SHARE_RAW_DATA.RAW_STATION_INFO
        );
      
  