/*
Here we see that:
- trip one lasted 1 minute
- second trip lastet 1 minute (and started 5 minutes after the first ended)
- third trip lasted 10 minutes (and started 30 minutes after second ended)

Our cutoff is one hours (60*60 seconds) , so we expect the see the return of row 1 only
*/


WITH cte AS (
    select 1 ID, 1 BIKE_ID, '2000-01-01 10:00:00'::TIMESTAMP_NTZ START_TIME, '2000-01-01 10:01:00'::TIMESTAMP_NTZ END_TIME
    UNION ALL
    select 2 ID, 1 BIKE_ID, '2000-01-01 10:06:00'::TIMESTAMP_NTZ START_TIME, '2000-01-01 10:07:00'::TIMESTAMP_NTZ END_TIME
    UNION ALL
    select 3 ID, 1 BIKE_ID, '2000-01-01 10:36:00'::TIMESTAMP_NTZ START_TIME, '2000-01-01 10:46:00'::TIMESTAMP_NTZ END_TIME
)
select
    cte.*
from cte, table(UDTF_PERFORMANCE_TESTING.SRC.PARTITION_CLEANER(cte.ID, cte.START_TIME, cte.END_TIME, 60*60) over (partition by cte.BIKE_ID order by cte.ID asc));