




SELECT 

  CAST(ParsedEventLog[OFFSET(2)] AS STRING) AS FK_ProfessionalID, -- ProfessionalID referenced as foreign key
  
  (CASE 
    WHEN SPLIT(ParsedEventLog[OFFSET(4)],'_')[OFFSET(0)]='' 
      THEN 'Not provided' 
      ELSE  SPLIT(ParsedEventLog[OFFSET(4)],'_')[OFFSET(0)] 
    END) AS FK_ServiceID, -- ServiceID referenced as foreign key
  
  ParsedEventLog[OFFSET(0)] AS EventID, -- EventID is always at 1st index (assuming 0 based indexing)
  ParsedEventLog[OFFSET(1)] AS EventName, -- EventName is always at 2nd index (assuming 0 based indexing)
  CAST(ParsedEventLog[OFFSET(3)] AS DATETIME) AS CreatedAt, -- CreatedAt is always at 3rd index (assuming 0 based indexing)
  
  (CASE 
    WHEN ParsedEventLog[OFFSET(4)]!='' 
      THEN CAST(SPLIT(ParsedEventLog[OFFSET(4)],'_')[OFFSET(3)] AS FLOAT64) 
      ELSE 0.0 
   END) as LeadFee,
  
  CURRENT_DATETIME() as AuditCreatedDateTime, -- Audit column
  CURRENT_DATETIME() as AuditModifiedDateTime  -- Audit column
FROM
(
  SELECT 
      SPLIT(EventLogEntry,';') as ParsedEventLog
    FROM 
     `poetic-genius-315513.events_information_staging.events_log_data_stg`

      -- this will only be applied on an incremental run & will filter data early
      -- `poetic-genius-315513`.`events_information`.`fct_fee` will give last run date which can then be used to pick CDC records daily
      
)