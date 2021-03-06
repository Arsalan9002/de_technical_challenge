{{ 
  config(
    materialized='incremental',
    sort=['ProfessionalID','ServiceID','EventID'], 
    partition_by={
      "field": "AuditCreatedDateTime",
      "data_type": "datetime",
      "granularity": "day"
    }
  ) 
}}



SELECT
  ProfessionalID,
  
  (CASE 
    WHEN ParsedMetadata[OFFSET(0)]='' 
      THEN 'Not provided' 
      ELSE  ParsedMetadata[OFFSET(0)]
    END
  ) AS ServiceID,
  
  EventID,
  EventType,
  CreatedAt,
  
  (CASE 
    WHEN ParsedMetadata[OFFSET(0)]!='' 
      THEN CAST(ParsedMetadata[OFFSET(3)] AS FLOAT64) 
      ELSE 0.0 
   END
  ) as LeadFee,

  AuditCreatedDatetime
 
FROM
(
  SELECT
      EventID,
      EventType,
      ProfessionalID,
      CreatedAt,
      SPLIT(Metadata,'_') as ParsedMetadata,
      CURRENT_DATETIME() as AuditCreatedDatetime, -- Audit column
    
    FROM 
    
  
      {{ref('event_logs_stg')}}
     
      -- this will only be applied on an incremental run & will filter data early
      -- {{this}} will give last run date which can then be used to pick CDC records daily
      {% if is_incremental() %}
        where AuditCreatedDatetime > (select max(AuditCreatedDatetime) from {{ this }})
      {% endif %}
      
      
)