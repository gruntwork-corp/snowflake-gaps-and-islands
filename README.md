# Snowflake Gaps and Islands

SQL problems where you need to iterate over each row and consider the previous rows' values to determine if the next row is in scope, is often referred to as a "gaps and islands" problem. \
This term is commonly used in SQL discussions to describe situations where you need to identify distinct groups (islands) of consecutive rows based on certain criteria, and also identify gaps between those groups.

## Problem Description
In the context of a customer support center's activity logging system, where actions performed by customer support agents (CSAs) are recorded as events, you need to identify and retain only the rows representing "distinct" operations performed by CSAs for each case. \
The problem stem out from the fact that the CSA may trigger many event whilst supporting the same customer in the same session. These actions are to be considered "duplicates", and we want to wash them out not to bloat our load counter. \
The goal is to estimate the load on the customer support center, and the approach to estimate load is by counting "one unit of load" as one event per CSA per case every 15 minutes. \
To achieve this, you must identify islands (group by CAS_ID, CASE_ID) of actions within a specified time frame and remove redundant actions within those islands.

### Example Dataset

|  ID | CSA_ID | CASE_ID | EVENT_TIMESTAMP     |
|-----|--------|---------|---------------------|
| 1   | a      | 1       | 2000-01-01 10:00:00 |
| 2   | a      | 1       | 2000-01-01 10:01:00 |
| 3   | a      | 2       | 2000-01-01 10:02:00 |
| 4   | b      | 1       | 2000-01-01 10:03:00 |
| 5   | a      | 1       | 2000-01-01 10:16:00 |
| 6   | b      | 2       | 2000-01-01 10:17:00 |
| 7   | b      | 1       | 2000-01-01 10:17:30 |

### Expected Output

|  ID | CSA_ID | CASE_ID | EVENT_TIMESTAMP     |
|-----|--------|---------|---------------------|
| 1   | a      | 1       | 2000-01-01 10:00:00 |
| 3   | a      | 2       | 2000-01-01 10:05:00 |
| 4   | b      | 1       | 2000-01-01 10:05:00 |
| 5   | a      | 1       | 2000-01-01 10:16:00 |
| 6   | b      | 2       | 2000-01-01 10:17:00 |

As we can see the following happens:

- We lose id 2 because CSAs **a** performed an action on case **1** 2 minutes earlier (previous action id 1 @10:00:00)
- We lose id 7 because CSAs **b** performed an action on case **1** 13 minutes earlier (previous action id 4 @10:05:00)


## Problem solving in Snowflake

To solve this gaps and islands problem in Snowflake we combine the usage of [user defined table function](https://docs.snowflake.com/en/sql-reference/functions-table) and [table literals](https://docs.snowflake.com/en/sql-reference/literals-table) in order to parse out the unwanted rows.

### User defined table function (UDTF)

In Snowflake, a User Defined Table Function (UDTF) allows the creation of custom functions that operate row-wise and can return multiple rows for each input row. This differs from a traditional User Defined Function (UDF) which returns a single value for each input row.

**Properties of UDTFs:**

1) Input-Output Mapping:
    - 1-to-N: For each input row, it can generate one or multiple output rows.
    - M-to-N: A set of rows (M) as input can produce a different set of rows (N) as output.
2) Invocation: UDTFs are invoked in the FROM clause, wrapped by the TABLE() keyword.
3) Implementation: Snowflake UDTFs can be defined using the Python or Java programming language.

To solve our gaps and islands problem, we utilize the M-to-N mapping: for each island of events, we want to return only the rows which we deem to be in scope by function of timestamp evaluation.

In order to solve this issue, we're going to use a python UTFT that looks something along these lines:


```python
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
```

Above you see our implementaion of a handler class in Python. This class will have methods Snowflake invokes when the UDTF is called.

- Initialization (__init__ method): Optional but can be used to initialize state for processing input partitions.
- Processing Input (process method): Mandatory. This processes each input row and returns tabular results as tuples. The row order is decided by your SQL `order by`
- End of Partition (end_partition method): Optional but can be used to finalize the processing of input partitions, and can return a table as tuples.

In our case we know that we have a timestamp submitted to our process method in a given order, and we want to compare if the current timestamp for this given event within this island is greater than our cutoff value. \
If that is the case, we yield the row and update our scope, so that the next row is compared against the value of this current rows (that was deemed to be in scope) timestamp value.

For more information about how to implement python UDTFs, see [this](https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-tabular-functions) Snowflake documentation.

### Invocation of UDTS with table function

Table functions, or table literals, in Snowflake are a way to specify a table or a placeholder value in SQL queries. \
The main advantage of table literals is to help with parallelization, especially when using UDTFs (User-Defined Table Functions) and partitioning.
Further, tohelp the SQL compiler recognize a table function as a source of rows (when utilizing UDTFs), Snowflake requires that the table function call be wrapped by the TABLE() keyword.

**Usage:**

- Can only be used in the FROM clause.
- For UDTFs, wrapping the function call with TABLE() helps Snowflake identify it as a source of rows. This becomes especially useful when you want to pass the output of one table function into another or when joining with another table.
- Partitioning allows Snowflake to divide up the workload to enhance performance. Partitioning also allows Snowflake to process all rows with a common characteristic together, letting results be based on the entire group rather than individual rows.

For the gaps and islands problem above, we have a scenario where you pass the output of one query (which identifies events customer support agent case action in scope) into a UDTF for further processing. \
By using table literals and partitioning, you can optimize the query execution and simplify the SQL logic.

```sql
select 
    s1.event_id as source_event_id,
    my_udf.event_id as udf_event_id,
    s1.event_timestamp
from
(
    select
        s.*
    from customer_support_agent_case_event_source as s
    where 
        s.timestamp::date >= '2000-01-01'
        and s.timestamp::date <= '2000-12-31'
)s1, table(my_udf(s1.event_timestamp, 15*60) over (partition by  s1.event_timestamp::date, s1.cas_id, s1.case_id order by s1.event_timestamp asc))

```

In the example above, the `partition by` clause is used to divide the rows based on specific columns (i.e. creating islands), and the ORDER BY clause within the OVER window function determines the order in which rows within those partitions are processed.
When the query is calculated (inside s1), that output is then partitioned and fed into our UDTF by function of`table(<UDTF>, over (partition by))`. These partitions then have `PartitionCleaner` __init__() method invoked, and each row is fed to that instanceses *process()* method resulting in a **for each row: <process>**

The wrapping of the udf-call into the table() enables us to use `my_udf.some_column` expression notation in the top level select (if it were to output additional columns) whilst we're still able to reference `s1` \ 
This pattern showcases the power of combining table literals with explicit partitioning in Snowflake.


See [this](https://docs.snowflake.com/en/sql-reference/functions-table#using-a-table-function) for more info on table function usage
See [this](https://docs.snowflake.com/en/sql-reference/functions-table#label-using-a-table-as-input-to-a-table-function) for more info on using a table as input to a table function.
See [this](https://docs.snowflake.com/en/developer-guide/udf/udf-calling-sql#table-functions-and-partitions) for more info on table function parallelization

### Limitations

Note that ```Table functions (UDTFs) have a limit of 500 input arguments (rows) and 500 output columns.```


## Test and performance stripts bootstrap

The bootstrap below creates a database, schema and a UDTF along with a stage mounting the [Citibike](https://citibikenyc.com/system-data) dataset. We also ingest citibike 2014-01 into a table that we'll query performance tests against. \
All scripts in both [tests](./tests/) and [performance](./performance/) rely on the following assets to have been bootstrapped:


Note that a x-small warehouse use about 1 minute 20 secs to ingest the ish 23.5 million rows.

```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
CREATE DATABASE UDTF_PERFORMANCE_TESTING DATA_RETENTION_TIME_IN_DAYS = 0;
CREATE SCHEMA UDTF_PERFORMANCE_TESTING.SRC;

USE SCHEMA UDTF_PERFORMANCE_TESTING.SRC;

/*### Create stage mounting the Citibike Dataset ###*/
create or replace stage UDTF_PERFORMANCE_TESTING.SRC.CITIBIKE_STAGE
  url = "s3://sfquickstarts/VHOL Snowflake for Data Lake/Data/"  
  file_format=(type=parquet);

/*### Create a table that we are to read selected fields into ###*/
CREATE OR REPLACE TABLE UDTF_PERFORMANCE_TESTING.SRC.CITYBIKE (
    ID NUMBER(38,0) AUTOINCREMENT(1,1),
    BIKE_ID number(38,0),
    TRIPDURATION_IN_SECONDS number(38,0),
    START_STATION_ID number(38,0),
    END_STATION_ID number(38,0),
    START_TIME TIMESTAMP_NTZ,
    END_TIME TIMESTAMP_NTZ
);

/*### Insert 2014-01 data to CITYBIKE table. A total of 23.605.017 records ###*/
insert into UDTF_PERFORMANCE_TESTING.SRC.CITYBIKE(
    BIKE_ID, 
    TRIPDURATION_IN_SECONDS, 
    START_STATION_ID, 
    END_STATION_ID, 
    START_TIME, 
    END_TIME
)
select 
    $1:BIKEID::NUMBER(9,0) BIKE_ID,
    $1:TRIPDURATION::NUMBER(9, 0) TRIPDURATION_IN_SECONDS,
    $1:START_STATION_ID::NUMBER(4, 0) START_STATION_ID,
    $1:END_STATION_ID::NUMBER(4, 0) END_STATION_ID,
    $1:STARTTIME::TIMESTAMP_NTZ START_TIME,
    $1:STOPTIME::TIMESTAMP_NTZ END_TIME
from @UDTF_PERFORMANCE_TESTING.SRC.CITIBIKE_STAGE/2014/1
;--limit 100;

/*### Define partition cleaner UDTF ###*/
create or replace function UDTF_PERFORMANCE_TESTING.SRC.PARTITION_CLEANER(id NUMBER, start_time TIMESTAMP_NTZ, end_time TIMESTAMP_NTZ, duration_in_seconds_treshold NUMBER)
returns table (id number)
language python
runtime_version=3.10
handler='PartitionCleaner'
as $$
from datetime import datetime
class PartitionCleaner:
    def __init__(self):
      self._last_time: datetime = None
    
    def process(self, id:int, start_time:datetime, end_time:datetime, duration_in_seconds_treshold:int):
      if self._last_time is None:
          self._last_time = end_time
          yield (id,)
      elif (start_time - self._last_time).total_seconds() > duration_in_seconds_treshold:
          self._last_time = end_time
          yield (id,)
$$;
```

### Inspection

```sql

/*### Inspect stage ###*/
list @UDTF_PERFORMANCE_TESTING.SRC.CITIBIKE_STAGE;

/*### Inspect 100 rows from 2014-01 ###*/
select $1 from @UDTF_PERFORMANCE_TESTING.SRC.CITIBIKE_STAGE/2014/1 limit 100;
/*
{   
    "BIKEID": 26525,   
    "BIRTH_YEAR": 1986,   
    "END_STATION_ID": 3687,   
    "GENDER": 2,   
    "PROGRAM_ID": 0,   
    "STARTTIME": "2019-04-01 00:18:45.624",   
    "START_STATION_ID": 533,   
    "STOPTIME": "2019-04-01 00:28:03.232",   
    "TRIPDURATION": 557,   
    "USERTYPE": "Subscriber" 
}
*/

/*### Count inserted rows ###*/
select count(1) from UDTF_PERFORMANCE_TESTING.SRC.CITYBIKE;
/*### View inserted rows ###*/
select * from UDTF_PERFORMANCE_TESTING.SRC.CITYBIKE order by BIKE_ID, START_TIME asc limit 100;
```

### Cleanup

```sql
DROP DATABASE UDTF_PERFORMANCE_TESTING;
```

