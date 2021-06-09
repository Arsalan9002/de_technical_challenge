{{ 
  config(
    materialized='incremental',
    partition_by={
      "field": "AuditCreatedDatetime",
      "data_type": "datetime",
      "granularity": "day"
    }
  ) 
}}


SELECT
  ParsedEventLogEntry[OFFSET(0)] AS EventID,
  ParsedEventLogEntry[OFFSET(1)] AS EventType,
  ParsedEventLogEntry[OFFSET(2)] AS ProfessionalID,
  ParsedEventLogEntry[OFFSET(3)] AS CreatedAt,
  ParsedEventLogEntry[OFFSET(4)] AS Metadata,
  CURRENT_DATETIME() AS AuditCreatedDatetime -- Audit Column, Used for Partitioning & Filtering Day in daily runs
FROM
(
  SELECT 
    
    SPLIT( EventLogEntry, ';') AS ParsedEventLogEntry -- Parsing Raw Event Log Entry on ; delimiter
  
  FROM `poetic-genius-315513.events_information.raw_event_logs`

    -- this will only be applied on an incremental run & will filter data early
    -- {{this}} will give last run date which can then be used to pick CDC records daily
    {% if is_incremental() %}
    	where PARSE_DATETIME('%Y-%m-%d %H:%M:%S',AuditCreatedDatetime) >
        (select MAX(AuditCreatedDatetime) from {{ this }})
    {% endif %}
)