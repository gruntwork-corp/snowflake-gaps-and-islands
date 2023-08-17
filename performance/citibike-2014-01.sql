/*
To view the cleaner in action filter the subquery for cb.bike_id = 16854 and only run the subquery to view the data before the cleaner is applied.
A x-small warehouse should be able to process the entire dataset in about 20 secs (if already started).
*/

select * 
from
(
    select 
        *,
        cb.start_time::date START_DATE
    from UDTF_PERFORMANCE_TESTING.SRC.CITYBIKE cb
    --where cb.bike_id = 16854
)s1, table(UDTF_PERFORMANCE_TESTING.SRC.PARTITION_CLEANER(s1.ID, s1.START_TIME, s1.END_TIME, 600) over (partition by  s1.START_DATE, s1.BIKE_ID order by s1.START_DATE asc))
order by s1.bike_id, s1.START_DATE asc;