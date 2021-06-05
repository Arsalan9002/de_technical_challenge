

SELECT  -- ADDING one extra select so that results could be sorted based on PK_ServiceID
  *
FROM 
(
  SELECT
    DISTINCT 
      ParsedMetdata[OFFSET(0)] AS PK_ServiceID,
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
        
    ) 
    WHERE ParsedEventLog!=''
  )
)