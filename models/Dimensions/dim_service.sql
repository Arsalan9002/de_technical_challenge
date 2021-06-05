{{ 
  config(
    materialized='incremental',
    unique_key ='PK_ServiceID',
    sort='PK_ServiceID',
    partition_by={
      "field": "AuditCreatedDateTime",
      "data_type": "datetime",
      "granularity": "day"
    }
  ) 
}}

SELECT  -- ADDING one extra select so that results could be sorted based on PK_ServiceID
  *
FROM 
(
  SELECT
    DISTINCT 
      CAST(ParsedMetdata[OFFSET(0)] AS STRING) AS PK_ServiceID,
      ParsedMetdata[OFFSET(1)] AS ServiceNameDutch,
      ParsedMetdata[OFFSET(2)] AS ServiceNameEnglish,

      CURRENT_DATETIME() as AuditCreatedDateTime, -- Audit column
      CURRENT_DATETIME() as AuditModifiedDateTime  -- Audit column
  FROM 
  (
    SELECT 
      -- Splitting metadata field further on underscore to get information about service and fees
      SPLIT(ParsedEventLog,'_') AS ParsedMetdata
    FROM 
    (
      SELECT 
        -- since we know that the position of metadata field is 4th (assuming 0 based index)
        SPLIT(EventLogEntry,';')[OFFSET(4)] as ParsedEventLog
      FROM 
       `poetic-genius-315513.events_information_staging.events_log_data_stg`

        -- this will only be applied on an incremental run & will filter data early
        -- {{this}} will give last run date which can then be used to pick CDC records daily
        {% if is_incremental() %}
          where PARSE_DATETIME('%Y-%m-%d %H:%M:%S', AuditCreatedDatetime) > 
            (select max(AuditCreatedDatetime) from {{ this }})
        {% endif %}
    )
    -- Discarding those rows where metadata is empty to reduce processing
    WHERE ParsedEventLog!=''
  )
)
