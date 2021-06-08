{{ 
  config(
    materialized='incremental',
    unique_key ='ServiceID',
    sort='ServiceID',
    partition_by={
      "field": "AuditCreatedDateTime",
      "data_type": "datetime",
      "granularity": "day"
    }
  ) 
}}


SELECT

  DISTINCT 

    CAST(ParsedMetdata[OFFSET(0)] AS STRING) AS ServiceID,
    ParsedMetdata[OFFSET(1)] AS ServiceNameDutch,
    ParsedMetdata[OFFSET(2)] AS ServiceNameEnglish,
    CURRENT_DATETIME() as AuditCreatedDateTime, -- Audit Column, Used for Partitioning & Filtering Day in daily runs

FROM 
(
  SELECT 
    
    SPLIT(Metadata,'_') AS ParsedMetdata,
    AuditCreatedDatetime
  
  FROM 
    {{ref('event_logs_stg')}}
     WHERE  Metadata!=''
)

  -- this will only be applied on an incremental run & will filter data early
  -- {{this}} will give last run date which can then be used to pick CDC records daily
  {% if is_incremental() %}
    where AuditCreatedDatetime > (select max(AuditCreatedDatetime) from {{ this }})
  {% endif %}


