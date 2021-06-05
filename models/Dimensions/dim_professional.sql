{{ 
  config(
    materialized='incremental',
    unique_key ='PK_ProfessionalID',
    sort='PK_ProfessionalID',

    partition_by ={
      "field":"AuditCreatedDateTime",
      "data_type":"datetime",
      "granularity":"day"
    }
  ) 

}}


SELECT
  DISTINCT 
    CAST(ParsedEventLog[OFFSET(2)] AS INT64) AS PK_ProfessionalID,
    CURRENT_DATETIME() as AuditCreatedDateTime,
    CURRENT_DATETIME() as AuditModifiedDateTime
FROM 
(

  SELECT 
    SPLIT(EventLogEntry,';') as ParsedEventLog
  FROM 
   `poetic-genius-315513.events_information_staging.events_log_data_stg`
      
      -- this filter will only be applied on an incremental run
      {% if is_incremental() %}
        where PARSE_DATETIME('%Y-%m-%d %H:%M:%S', AuditCreatedDatetime) > 
          (select max(AuditCreatedDatetime) from {{ this }})
      {% endif %}

)

