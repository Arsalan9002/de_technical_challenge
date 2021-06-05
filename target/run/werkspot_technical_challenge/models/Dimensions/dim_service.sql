

  create or replace table `poetic-genius-315513`.`events_information`.`dim_service`
  partition by datetime_trunc(AuditCreatedDateTime, day)
  
  OPTIONS()
  as (
    


SELECT

  DISTINCT 

    CAST(ParsedMetdata[OFFSET(0)] AS STRING) AS ServiceID,
    ParsedMetdata[OFFSET(1)] AS ServiceNameDutch,
    ParsedMetdata[OFFSET(2)] AS ServiceNameEnglish,
    CURRENT_DATETIME() as AuditCreatedDateTime, -- Audit Column, Used for Partitioning & Filtering Day in daily runs

FROM 
(
  SELECT 
    
    SPLIT(Metadata,'_') AS ParsedMetdata
  
  FROM 
    `poetic-genius-315513.events_information.event_logs_stg`
     WHERE  Metadata!=''
)

  -- this will only be applied on an incremental run & will filter data early
  -- `poetic-genius-315513`.`events_information`.`dim_service` will give last run date which can then be used to pick CDC records daily
  
  );
  