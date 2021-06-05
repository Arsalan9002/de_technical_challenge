{{ 
  config(
    materialized='incremental',
    unique_key ='ProfessionalID',
    sort='ProfessionalID', 
    partition_by={
      "field": "AuditCreatedDateTime",
      "data_type": "datetime",
      "granularity": "day"
    }
  ) 
}}


SELECT
  
  DISTINCT

    ProfessionalID,
    CURRENT_DATETIME() as AuditCreatedDateTime,  -- Audit Column, Used for Partitioning & Filtering Day in daily runs

FROM 

  {{ref('event_logs_stg')}}
      
    -- this will only be applied on an incremental run & will filter data early
    -- {{this}} will give last run date which can then be used to pick CDC records daily
    {% if is_incremental() %}
      where PARSE_DATETIME('%Y-%m-%d %H:%M:%S', AuditCreatedDatetime) > 
        (select max(AuditCreatedDatetime) from {{ this }})
    {% endif %}



