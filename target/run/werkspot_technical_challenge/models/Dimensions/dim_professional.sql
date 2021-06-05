

  create or replace table `poetic-genius-315513`.`events_information`.`dim_professional`
  partition by datetime_trunc(AuditCreatedDateTime, day)
  
  OPTIONS()
  as (
    


SELECT
  
  DISTINCT

    ProfessionalID,
    CURRENT_DATETIME() as AuditCreatedDateTime,  -- Audit Column, Used for Partitioning & Filtering Day in daily runs

FROM 

  `poetic-genius-315513`.`events_information`.`event_logs_stg`
      
    -- this will only be applied on an incremental run & will filter data early
    -- `poetic-genius-315513`.`events_information`.`dim_professional` will give last run date which can then be used to pick CDC records daily
    
  );
  