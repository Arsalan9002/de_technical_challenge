
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

SELECT
  DelimitedEventLog[OFFSET(0)] AS EevntID,
  DelimitedEventLog[OFFSET(1)] AS EventType,
  DelimitedEventLog[OFFSET(2)] AS ProfessionalID,
  DelimitedEventLog[OFFSET(3)] AS CreatedAt,
  DelimitedEventLog[OFFSET(4)] AS Metadata
FROM (
  SELECT 
    SPLIT( EventLogEntry, ';') AS DelimitedEventLog
  FROM `poetic-genius-315513.events_information_staging.events_log_data_stg`
)
