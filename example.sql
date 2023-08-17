USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

create or replace function UDTF_PERFORMANCE_TESTING.SRC.EVENT_TIMESTAMP_CLEANER(src_timestamp TIMESTAMP_NTZ, duration_in_seconds_treshold NUMBER(38,0))
returns table (id number)
language python
runtime_version=3.10
handler='PartitionCleaner'
as $$
from datetime import datetime
class PartitionCleaner:
    def __init__(self):
      self._scope_timestamp: datetime = None
    
    def process(self, src_timestamp:datetime, duration_in_seconds_treshold:int):
      if self._scope_timestamp is None:
          self._scope_timestamp = src_timestamp
          yield (True,)
      elif (src_timestamp - self._scope_timestamp).total_seconds() > duration_in_seconds_treshold:
          self._scope_timestamp = src_timestamp
          yield (True,)
$$;

WITH cte AS (
    SELECT 1 AS ID, 'a' AS CAS_ID, 1 AS CASE_ID, '2000-01-01 10:00:00'::TIMESTAMP_NTZ AS EVENT_TIMESTAMP
    UNION ALL
    SELECT 2, 'a', 1, '2000-01-01 10:01:00'::TIMESTAMP_NTZ
    UNION ALL
    SELECT 3, 'a', 2, '2000-01-01 10:02:00'::TIMESTAMP_NTZ  
    UNION ALL
    SELECT 4, 'b', 1, '2000-01-01 10:03:00'::TIMESTAMP_NTZ
    UNION ALL
    SELECT 5, 'a', 1, '2000-01-01 10:15:00'::TIMESTAMP_NTZ
    UNION ALL
    SELECT 6, 'b', 2, '2000-01-01 10:17:00'::TIMESTAMP_NTZ
    UNION ALL
    SELECT 7, 'b', 1, '2000-01-01 10:17:30'::TIMESTAMP_NTZ
)
SELECT 
    cte.*,
    t.* 
FROM cte, 
    table(UDTF_PERFORMANCE_TESTING.SRC.EVENT_TIMESTAMP_CLEANER(cte.event_timestamp, 15*60) over(partition by cte.cas_id, cte.case_id order by cte.event_timestamp asc)) t
order by
    cte.event_timestamp asc;