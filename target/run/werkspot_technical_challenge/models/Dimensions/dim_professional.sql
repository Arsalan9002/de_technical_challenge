

  create or replace table `poetic-genius-315513`.`events_information`.`dim_professional`
  partition by datetime_trunc(AuditCreatedDateTime, day)
  
  OPTIONS()
  as (
    


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
      -- `poetic-genius-315513`.`events_information`.`dim_professional` will give last run date which can then be used to pick CDC records daily
      

)
  );
  