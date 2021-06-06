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

  CreatedAt,
  COUNT(DISTINCT CASE WHEN ProfessionalStatus IS TRUE THEN ProfessionalID END) AS ActiveProfessionals

FROM 
(
  SELECT
    *,
    CASE
      WHEN 
        EventType IN ('created_account','became_unable_to_propose') AND NextEvent IS NULL 
        THEN False 
      WHEN EventType IN ('created_account','became_able_to_propose') AND NextEvent='became_unable_to_propose' 
        THEN False 
      WHEN EventType IN ('proposed','became_unable_to_propose') AND (NextEvent='became_unable_to_propose') 
        THEN False
      
      WHEN EventType IN ('proposed','became_able_to_propose','became_unable_to_propose') AND 
           NextEvent IN ('proposed','became_able_to_propose') 
        THEN True
      WHEN EventType IN ('proposed','became_able_to_propose','became_unable_to_propose') AND 
           NextEvent IN ('proposed','became_able_to_propose') 
        THEN True
      WHEN EventType IN ('created_account','became_able_to_propose','proposed') AND 
           (NextEvent IS NULL OR NextEvent='became_able_to_propose') 
        THEN True
    END AS ProfessionalStatus
  
  FROM 
  (
    SELECT

     DISTINCT
      EXTRACT(DATE FROM CAST(CreatedAt AS DATETIME)) AS CreatedAt,
      ProfessionalID,
      EventType,
      LEAD(EventType,1 ) OVER 
          (PARTITION By EXTRACT(DATE FROM CAST(CreatedAt AS DATETIME)),ProfessionalID 
            ORDER BY CreatedAt ASC) as NextEvent
    FROM
    (
      SELECT 

        DISTINCT
          CreatedAt,
          ProfessionalID,
          EventType

      FROM 
        {{ref('event_logs_stg')}}
        
      GROUP BY 1,2,3 
      HAVING CreatedAt BETWEEN CreatedAt AND '2020-03-10 23:59:59'
    )
  )
) 
GROUP BY 1 ORDER BY 1 ASC;