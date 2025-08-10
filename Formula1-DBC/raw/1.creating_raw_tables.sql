-- Databricks notebook source
Create Database if not exists f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###create circuits table

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits( circuitId int,
circuitRef string,
name string,
location string,
country string,
lat double,
lng double,
alt int,
url string
) using csv options(path "/mnt/formula121/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Creating Races table

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races( raceId int,
year int,
round int,
circuitId int,
name string,
date date,
time string,
url string
) using csv options(path "/mnt/formula121/raw/races.csv", header true)

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Creating Tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Creating Constructors table
-- MAGIC 1.single Line JSON
-- MAGIC
-- MAGIC 2.Simple structure

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors( constructorId int,
constructorRef string,
name string,
nationality string,
url string
) using json options(path "/mnt/formula121/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC ###Creating drivers table
-- MAGIC 1.single Line JSON
-- MAGIC
-- MAGIC 2.Complex structure

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers( driverId int,
driverRef string,
number int,
code string,
name struct<forename:string, surname:string>,
dob date,
nationality string,
url string
) using json options(path "/mnt/formula121/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Creating results table
-- MAGIC 1.single Line JSON
-- MAGIC
-- MAGIC 2.Simple Structure

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results( resultId int,
raceId int,
driverId int,
constructorId int,
number int,
grid int,
position int,
positionText string,
positionOrder int,
points int,
laps int,
time string,
milliseconds int,
fastestLap int,
rank int,
fastestLapTime string,
fastestLapSpeed float,
statusId string
) using json options(path "/mnt/formula121/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Creating pit stops table
-- MAGIC 1.Multi Line JSON
-- MAGIC
-- MAGIC 2.Simple Structure

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops( driverId int,
duration string,
lap int,
milliseconds int,
raceId int,
stop int,
time string
) using json options(path "/mnt/formula121/raw/pit_stops.json", multiline true)

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Creating tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Creating Lap Times Table
-- MAGIC 1. CSV file
-- MAGIC 2. Multiple files

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
raceId int,
driverId int,
lap int ,
position int,
time int,
milliseconds int
) using csv options(path "/mnt/formula121/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating Qualifying Tbale
-- MAGIC 1. JSON file
-- MAGIC 2. Multiline JSON
-- MAGIC 3. Multiple Files

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
constructorId int,
driverId int,
number int,
position int,
q1 string,
q2 string,
q3 string,
qualifyId int,
raceId int
) using json options(path "/mnt/formula121/raw/qualifying", multiline true)

-- COMMAND ----------

select * from f1_raw.qualifying;