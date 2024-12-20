-- Databricks notebook source
-- MAGIC %md
-- MAGIC **NOTE : Using LIMIT in every query just for maintaining the notebook**

-- COMMAND ----------

select * from demo_db.fire_service_calls_tbl limit 5

-- COMMAND ----------

select count(*) from demo_db.fire_service_calls_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q1. How many distinct types of calls were made to the Fire Department?

-- COMMAND ----------

select count(distinct callType) as distinct_call_type_count
from demo_db.fire_service_calls_tbl
where callType is not null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q2. What were distinct types of calls made to the Fire Department?

-- COMMAND ----------

select distinct callType as distinct_call_types
from demo_db.fire_service_calls_tbl
where callType is not null
limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q3. Find out all response for delayed times greater than 5 mins?

-- COMMAND ----------

select callNumber, Delay
from demo_db.fire_service_calls_tbl
where Delay > 5
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q4. What were the most common call types?

-- COMMAND ----------

select callType, count(*) as count
from demo_db.fire_service_calls_tbl
where callType is not null
group by callType
order by count desc
limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q5. What zip codes accounted for most common calls?

-- COMMAND ----------

select callType, zipCode, count(*) as count
from demo_db.fire_service_calls_tbl
where callType is not null
group by callType, zipCode
order by count desc
limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q6. What San Francisco neighborhoods are in the zip codes 94102 and 94103?

-- COMMAND ----------

select zipCode, neighborhood
from demo_db.fire_service_calls_tbl
where zipCode == 94102 or zipCode == 94103
limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Q7. What was the sum of all call alarms, average, min, and max of the call response times?
-- MAGIC

-- COMMAND ----------

select sum(NumAlarms), avg(Delay), min(Delay), max(Delay)
from demo_db.fire_service_calls_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q8. How many distinct years of data is in the data set?
-- MAGIC

-- COMMAND ----------

SELECT DISTINCT year(to_date(callDate, 'yyyy-MM-dd')) AS year_num
FROM demo_db.fire_service_calls_tbl
ORDER BY year_num
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q9. What week of the year in 2018 had the most fire calls?

-- COMMAND ----------

select weekofyear(to_date(callDate, 'yyyy-MM-dd')) week_year, count(*) as count
from demo_db.fire_service_calls_tbl
where year(to_date(callDate, 'yyyy-MM-dd')) == 2018
group by week_year
order by count desc
limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Q10. What neighborhoods in San Francisco had the worst response time in 2018?

-- COMMAND ----------

select neighborhood, delay
from demo_db.fire_service_calls_tbl
where year(to_date(callDate, "yyyy-MM-dd")) == 2018
order by delay desc
limit 5
