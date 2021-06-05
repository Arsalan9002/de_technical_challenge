{{ 
  config(
    materialized='incremental',
    unique_key ='PK_ProfessionalID',
    sort='PK_ProfessionalID', 
    partition_by={
      "field": "AuditCreatedDateTime",
      "data_type": "datetime",
      "granularity": "day"
    }
  ) 
}}


SELECT
  DISTINCT 
    CAST(ParsedEventLog[OFFSET(2)] AS STRING) AS PK_ProfessionalID,
    CURRENT_DATETIME() as AuditCreatedDateTime,  -- Audit column
    CURRENT_DATETIME() as AuditModifiedDateTime  -- Audit column
FROM 
(

  SELECT 
    SPLIT(EventLogEntry,';') as ParsedEventLog
  FROM 
   `poetic-genius-315513.events_information_staging.events_log_data_stg`
      
      -- this will only be applied on an incremental run & will filter data early
      -- {{this}} will give last run date which can then be used to pick CDC records daily
      {% if is_incremental() %}
        where PARSE_DATETIME('%Y-%m-%d %H:%M:%S', AuditCreatedDatetime) > 
          (select max(AuditCreatedDatetime) from {{ this }})
      {% endif %}

)

