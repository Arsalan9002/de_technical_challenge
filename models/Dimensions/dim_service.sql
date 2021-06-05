{{ 
  config(
    materialized='incremental',
    unique_key ='PK_ServiceID',
    sort='PK_ServiceID',

    partition_by ={
      "field":"AuditCreatedDateTime",
      "data_type":"datetime",
      "granularity":"day"
    }
  ) 

}}

SELECT  -- ADDING one extra select so that results could be sorted based on PK_ServiceID
  *
FROM 
(
  SELECT
    DISTINCT 
      CAST(ParsedMetdata[OFFSET(0)] AS INT64) AS PK_ServiceID,
      ParsedMetdata[OFFSET(1)] AS ServiceNameDutch,
      ParsedMetdata[OFFSET(2)] AS ServiceNameEnglish,
      CURRENT_DATETIME() as AuditCreatedDateTime,
      CURRENT_DATETIME() as AuditModifiedDateTime
  FROM 
  (
    SELECT 
      SPLIT(ParsedEventLog,'_') AS ParsedMetdata
    FROM 
    (
      SELECT 
        SPLIT(EventLogEntry,';')[OFFSET(4)] as ParsedEventLog
      FROM 
       `poetic-genius-315513.events_information_staging.events_log_data_stg`

        -- this filter will only be applied on an incremental run
        {% if is_incremental() %}
          where PARSE_DATETIME('%Y-%m-%d %H:%M:%S', AuditCreatedDatetime) > 
            (select max(AuditCreatedDatetime) from {{ this }})
        {% endif %}
    ) 
    WHERE ParsedEventLog!=''
  )
)
