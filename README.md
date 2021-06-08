### Technical Challenge

Following commands are used in DBT Cloud to automate the solution,
dbt run --models event_logs_stg
dbt run --models dim_professional dim_service fct_fee
dbt run --models availibility_snapshot


### Dimensions
Professional
Service

### Fact(s)
Fee

### Snapshot
availibilty_snapshot
- Date range of data in snapshot table is between minimum event time and 2021-03-10
- Professionals are considered as Active if they perform ‘became_able_to_propose’ event and will be considered as In Active if they perform ‘became_not_able_to_propose’ event
- If professionals perform both ‘became_able_to_propose’ and ‘became_not_able_to_propose’ in a single day at different time intervals then their active/in active status would be decided based on the very first event they performed on that specific day.
