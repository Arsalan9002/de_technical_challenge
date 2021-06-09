{{ 
  config(
    materialized='table'
  ) 
}}


SELECT
  CreatedAt,
  COUNT(DISTINCT CASE WHEN ProfessionalStatus IS TRUE THEN ProfessionalID END) AS ActiveProfessionals
FROM
(
  SELECT 
    DISTINCT
      EXTRACT(DATE FROM CREATEDAT) AS CreatedAt,
      Professionalid,
      FIRST_VALUE(ProfessionalStatus IGNORE NULLS) OVER 
        (PARTITION BY extract(date from cast(CreatedAt as datetime)), PROFESSIONALID ORDER BY cast(CreatedAt as datetime)
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ProfessionalStatus
  FROM 
  (
    select
      *,
      CASE   
        WHEN 
          EventType IN ('became_unable_to_propose') AND (NextEvent IS NULL) OR  
          NEXTEVENT='became_unable_to_propose'
          THEN False 

        WHEN 
          EventType IN ('became_able_to_propose') AND 
          (NextEvent IS NULL OR NextEvent='proposed') OR NEXTEVENT='became_able_to_propose'
          THEN TRUE

        WHEN 
          EventType='proposed' AND NextEvent IS NULL
          THEN TRUE
      END AS ProfessionalStatus

    FROM 
    (
      SELECT
       DISTINCT
        CAST(CreatedAt AS DATETIME) AS CreatedAt,
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
        HAVING CreatedAt BETWEEN MIN(CreatedAt) AND '2020-03-10 23:59:59'
      )
    )
  )
  ORDER BY 1,2
)
GROUP BY 1
ORDER BY 1 ASC