


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
      

)