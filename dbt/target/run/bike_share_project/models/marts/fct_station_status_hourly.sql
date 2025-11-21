-- back compat for old kwarg name
  
  begin;
    
        
            
	    
	    
            
        
    

    

    merge into BIKE_SHARE_DB.BIKE_SHARE_ANALYTICS.fct_station_status_hourly as DBT_INTERNAL_DEST
        using BIKE_SHARE_DB.BIKE_SHARE_ANALYTICS.fct_station_status_hourly__dbt_tmp as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.station_id_hour_ts = DBT_INTERNAL_DEST.station_id_hour_ts))

    
    when matched then update set
        "STATION_ID" = DBT_INTERNAL_SOURCE."STATION_ID","HOUR_TS" = DBT_INTERNAL_SOURCE."HOUR_TS","AVG_BIKES_AVAILABLE" = DBT_INTERNAL_SOURCE."AVG_BIKES_AVAILABLE","AVG_DOCKS_AVAILABLE" = DBT_INTERNAL_SOURCE."AVG_DOCKS_AVAILABLE","LAST_REPORTED_TS" = DBT_INTERNAL_SOURCE."LAST_REPORTED_TS","STATION_ID_HOUR_TS" = DBT_INTERNAL_SOURCE."STATION_ID_HOUR_TS"
    

    when not matched then insert
        ("STATION_ID", "HOUR_TS", "AVG_BIKES_AVAILABLE", "AVG_DOCKS_AVAILABLE", "LAST_REPORTED_TS", "STATION_ID_HOUR_TS")
    values
        ("STATION_ID", "HOUR_TS", "AVG_BIKES_AVAILABLE", "AVG_DOCKS_AVAILABLE", "LAST_REPORTED_TS", "STATION_ID_HOUR_TS")

;
    commit;