-- STEP 1: Create Database
CREATE DATABASE Airline_Performance_Analysis;

-- STEP 2: Use the Database
USE Airline_Performance_Analysis;

-- STEP 3: Create Tables

-- Airlines table
DROP TABLE IF EXISTS dbo.airlines;
CREATE TABLE dbo.airlines (
    IATA_CODE VARCHAR(10),
    AIRLINE NVARCHAR(255)
);

-- Airports table
DROP TABLE IF EXISTS dbo.airports;
CREATE TABLE dbo.airports (
    IATA_CODE VARCHAR(10),
    AIRPORT NVARCHAR(255),
    CITY NVARCHAR(255),
    STATE NVARCHAR(100),
    COUNTRY VARCHAR(100),
    LATITUDE FLOAT,
    LONGITUDE FLOAT
);

-- Flights table
DROP TABLE IF EXISTS dbo.flights_raw;
CREATE TABLE dbo.flights_raw (
    [YEAR] INT,
    [MONTH] INT,
    [DAY] INT,
    DAY_OF_WEEK INT,
    AIRLINE VARCHAR(10),
    FLIGHT_NUMBER NVARCHAR(20),
    TAIL_NUMBER NVARCHAR(20),
    ORIGIN_AIRPORT VARCHAR(10),
    DESTINATION_AIRPORT VARCHAR(10),
    SCHEDULED_DEPARTURE NVARCHAR(10),
    DEPARTURE_TIME NVARCHAR(10),
    DEPARTURE_DELAY FLOAT,
    TAXI_OUT FLOAT,
    WHEELS_OFF NVARCHAR(20),
    SCHEDULED_TIME FLOAT,
    ELAPSED_TIME FLOAT,
    AIR_TIME FLOAT,
    DISTANCE FLOAT,
    WHEELS_ON NVARCHAR(20),
    TAXI_IN FLOAT,
    SCHEDULED_ARRIVAL NVARCHAR(10),
    ARRIVAL_TIME NVARCHAR(10),
    ARRIVAL_DELAY FLOAT,
    DIVERTED BIT,
    CANCELLED BIT,
    CANCELLATION_REASON NVARCHAR(50),
    AIR_SYSTEM_DELAY FLOAT,
    SECURITY_DELAY FLOAT,
    AIRLINE_DELAY FLOAT,
    LATE_AIRCRAFT_DELAY FLOAT,
    WEATHER_DELAY FLOAT
);


BULK INSERT dbo.airlines
FROM 'C:\CSVData\airlines.csv'
WITH (
    FIRSTROW = 2,              -- skip header
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',    -- LF only
    TABLOCK
);

BULK INSERT dbo.airports
FROM 'C:\CSVData\airports.csv'
WITH (
    FIRSTROW = 2,              -- skip header
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',    -- LF only
    TABLOCK
);

BULK INSERT dbo.flights_raw
FROM 'C:\CSVData\flights.csv'
WITH (
    FIRSTROW = 2,              -- skip header
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',    -- LF only
    TABLOCK,
    MAXERRORS = 1000           -- skips up to 1000 bad rows
);


SELECT COUNT(*) AS airlines_count FROM dbo.airlines;
SELECT COUNT(*) AS airports_count FROM dbo.airports;
SELECT COUNT(*) AS flights_count FROM dbo.flights_raw;

SELECT TOP 5 * FROM dbo.airlines;
SELECT TOP 5 * FROM dbo.airports;
SELECT TOP 5 * FROM dbo.flights_raw;


-- Check delays and cancellations
SELECT COUNT(*) AS null_departure_delay FROM dbo.flights_raw WHERE DEPARTURE_DELAY IS NULL;
SELECT COUNT(*) AS null_arrival_delay FROM dbo.flights_raw WHERE ARRIVAL_DELAY IS NULL;
SELECT COUNT(*) AS cancelled_flights FROM dbo.flights_raw WHERE CANCELLED = 1;



SELECT TOP 10 * 
FROM dbo.vw_flights_full;

--Phase 2: Data Cleaning, Preparation & Integration

-- Add the FLIGHT_DATE column to flights_raw
ALTER TABLE dbo.flights_raw
ADD FLIGHT_DATE DATE;

--Populate FLIGHT_DATE from year, month, day
UPDATE dbo.flights_raw
SET FLIGHT_DATE = DATEFROMPARTS([YEAR], [MONTH], [DAY]);

--create the view
DROP VIEW IF EXISTS dbo.vw_flights_full;

CREATE VIEW dbo.vw_flights_full AS
SELECT f.*, 
       a1.AIRPORT AS ORIGIN_AIRPORT_NAME, a1.CITY AS ORIGIN_CITY, a1.STATE AS ORIGIN_STATE,
       a2.AIRPORT AS DEST_AIRPORT_NAME, a2.CITY AS DEST_CITY, a2.STATE AS DEST_STATE,
       al.AIRLINE AS AIRLINE_NAME
FROM dbo.flights_raw f
LEFT JOIN dbo.airports a1 ON f.ORIGIN_AIRPORT = a1.IATA_CODE
LEFT JOIN dbo.airports a2 ON f.DESTINATION_AIRPORT = a2.IATA_CODE
LEFT JOIN dbo.airlines al ON f.AIRLINE = al.IATA_CODE;

SELECT TOP 10 * 
FROM dbo.vw_flights_full;

--Handle Date & Time Columns


--Check existing data
SELECT TOP 10 YEAR, MONTH, DAY FROM dbo.flights_raw;
SELECT TOP 10 FLIGHT_DATE FROM dbo.flights_raw;

--Update FLIGHT_DATE if already exists
UPDATE dbo.flights_raw
SET FLIGHT_DATE = TRY_CONVERT(
    DATE,
    CONCAT(
        YEAR,
        RIGHT('0' + CAST(MONTH AS VARCHAR(2)), 2),
        RIGHT('0' + CAST(DAY AS VARCHAR(2)), 2)
    )
)
WHERE FLIGHT_DATE IS NULL
AND YEAR IS NOT NULL
AND MONTH IS NOT NULL
AND DAY IS NOT NULL;

--Add TIME columns safely
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'flights_raw';

    -- Add DATETIME columns only if they do not exist
IF COL_LENGTH('dbo.flights_raw', 'SCHEDULED_DEP_DATETIME') IS NULL
    ALTER TABLE dbo.flights_raw ADD SCHEDULED_DEP_DATETIME DATETIME;

IF COL_LENGTH('dbo.flights_raw', 'SCHEDULED_ARR_DATETIME') IS NULL
    ALTER TABLE dbo.flights_raw ADD SCHEDULED_ARR_DATETIME DATETIME;

IF COL_LENGTH('dbo.flights_raw', 'ACTUAL_DEP_DATETIME') IS NULL
    ALTER TABLE dbo.flights_raw ADD ACTUAL_DEP_DATETIME DATETIME;

IF COL_LENGTH('dbo.flights_raw', 'ACTUAL_ARR_DATETIME') IS NULL
    ALTER TABLE dbo.flights_raw ADD ACTUAL_ARR_DATETIME DATETIME;

--Populate DATETIME columns safely
UPDATE dbo.flights_raw
SET
    SCHEDULED_DEP_DATETIME = CASE 
        WHEN SCHEDULED_DEP_TIME IS NOT NULL AND FLIGHT_DATE IS NOT NULL
        THEN CAST(FLIGHT_DATE AS DATETIME) + CAST(SCHEDULED_DEP_TIME AS DATETIME)
        ELSE NULL
    END,
    SCHEDULED_ARR_DATETIME = CASE 
        WHEN SCHEDULED_ARR_TIME IS NOT NULL AND FLIGHT_DATE IS NOT NULL
        THEN CAST(FLIGHT_DATE AS DATETIME) + CAST(SCHEDULED_ARR_TIME AS DATETIME)
        ELSE NULL
    END,
    ACTUAL_DEP_DATETIME = CASE 
        WHEN ACTUAL_DEP_TIME IS NOT NULL AND FLIGHT_DATE IS NOT NULL
        THEN CAST(FLIGHT_DATE AS DATETIME) + CAST(ACTUAL_DEP_TIME AS DATETIME)
        ELSE NULL
    END,
    ACTUAL_ARR_DATETIME = CASE 
        WHEN ACTUAL_ARR_TIME IS NOT NULL AND FLIGHT_DATE IS NOT NULL
        THEN CAST(FLIGHT_DATE AS DATETIME) + CAST(ACTUAL_ARR_TIME AS DATETIME)
        ELSE NULL
    END;


SELECT TOP 10 
    FLIGHT_DATE,
    SCHEDULED_DEP_TIME, SCHEDULED_ARR_TIME,
    ACTUAL_DEP_TIME, ACTUAL_ARR_TIME,
    SCHEDULED_DEP_DATETIME, SCHEDULED_ARR_DATETIME,
    ACTUAL_DEP_DATETIME, ACTUAL_ARR_DATETIME
FROM dbo.flights_raw;

-- 1. Add the descriptive cancellation reason column
IF COL_LENGTH('dbo.flights_raw', 'CANCELLATION_REASON_DESC') IS NULL
BEGIN
    ALTER TABLE dbo.flights_raw
    ADD CANCELLATION_REASON_DESC VARCHAR(50);
END

-- 2. Populate it based on CANCELLATION_REASON codes
UPDATE dbo.flights_raw
SET CANCELLATION_REASON_DESC = CASE 
    WHEN CANCELLATION_REASON = 'A' THEN 'Airline/Carrier'
    WHEN CANCELLATION_REASON = 'B' THEN 'Weather'
    WHEN CANCELLATION_REASON = 'C' THEN 'National Air System (NAS)'
    WHEN CANCELLATION_REASON = 'D' THEN 'Security'
    ELSE 'Not Cancelled'
END;

--Phase 3: Exploratory Data Analysis (EDA) & KPI Definition
--Check overall flight statistics
-- Total flights
SELECT COUNT(*) AS Total_Flights FROM dbo.flights_raw;

-- Total cancelled flights
SELECT COUNT(*) AS Cancelled_Flights 
FROM dbo.flights_raw
WHERE CANCELLED = 1;

-- Total diverted flights (if there is a DIVERTED column)
SELECT COUNT(*) AS Diverted_Flights
FROM dbo.flights_raw
WHERE DIVERTED = 1;

--Basic delay statistics
-- Departure delays
SELECT 
    AVG(DEPARTURE_DELAY) AS Avg_Departure_Delay,
    MIN(DEPARTURE_DELAY) AS Min_Departure_Delay,
    MAX(DEPARTURE_DELAY) AS Max_Departure_Delay
FROM dbo.flights_raw
WHERE DEPARTURE_DELAY IS NOT NULL;

-- Arrival delays
SELECT 
    AVG(ARRIVAL_DELAY) AS Avg_Arrival_Delay,
    MIN(ARRIVAL_DELAY) AS Min_Arrival_Delay,
    MAX(ARRIVAL_DELAY) AS Max_Arrival_Delay
FROM dbo.flights_raw
WHERE ARRIVAL_DELAY IS NOT NULL;

--Distribution of cancellation reasons
SELECT CANCELLATION_REASON_DESC, COUNT(*) AS Count
FROM dbo.flights_raw
WHERE CANCELLED = 1
GROUP BY CANCELLATION_REASON_DESC
ORDER BY Count DESC;

--KPI calculations (examples)
-- On-Time Performance (OTP)
SELECT 
    AIRLINE,
    SUM(CASE WHEN ARRIVAL_DELAY <= 15 THEN 1 ELSE 0 END)*1.0/COUNT(*) AS OnTime_Percentage
FROM dbo.flights_raw
GROUP BY AIRLINE;

-- Average Delay per Airline
SELECT 
    AIRLINE,
    AVG(DEPARTURE_DELAY) AS Avg_Departure_Delay,
    AVG(ARRIVAL_DELAY) AS Avg_Arrival_Delay
FROM dbo.flights_raw
GROUP BY AIRLINE;

--Trends by time
-- Monthly delays
SELECT MONTH, AVG(DEPARTURE_DELAY) AS Avg_Departure_Delay, AVG(ARRIVAL_DELAY) AS Avg_Arrival_Delay
FROM dbo.flights_raw
GROUP BY MONTH
ORDER BY MONTH;

-- Day-of-week analysis
SELECT DAY_OF_WEEK, AVG(DEPARTURE_DELAY) AS Avg_Departure_Delay, AVG(ARRIVAL_DELAY) AS Avg_Arrival_Delay
FROM dbo.flights_raw
GROUP BY DAY_OF_WEEK
ORDER BY DAY_OF_WEEK;

-- PHASE 4–6: BI-READY DATA, KPI VIEWS & ENRICHMENTS
USE Airline_Performance_Analysis;

-- 1️ Add Flight Hour & Part of Day Columns
-------------------------------------------------
IF COL_LENGTH('dbo.flights_raw', 'SCHEDULED_DEP_HOUR') IS NULL
    ALTER TABLE dbo.flights_raw ADD SCHEDULED_DEP_HOUR INT;

UPDATE dbo.flights_raw
SET SCHEDULED_DEP_HOUR = CAST(LEFT(RIGHT('0000' + SCHEDULED_DEPARTURE, 4), 2) AS INT);

IF COL_LENGTH('dbo.flights_raw', 'PART_OF_DAY') IS NULL
    ALTER TABLE dbo.flights_raw ADD PART_OF_DAY VARCHAR(20);

UPDATE dbo.flights_raw
SET PART_OF_DAY = CASE
    WHEN SCHEDULED_DEP_HOUR BETWEEN 5 AND 11 THEN 'Morning'
    WHEN SCHEDULED_DEP_HOUR BETWEEN 12 AND 16 THEN 'Afternoon'
    WHEN SCHEDULED_DEP_HOUR BETWEEN 17 AND 20 THEN 'Evening'
    ELSE 'Night'
END;

-------------------------------------------------
-- 2️ Add Route Column (Origin → Destination)
-------------------------------------------------
IF COL_LENGTH('dbo.flights_raw', 'ROUTE') IS NULL
    ALTER TABLE dbo.flights_raw ADD ROUTE VARCHAR(50);

UPDATE dbo.flights_raw
SET ROUTE = ORIGIN_AIRPORT + ' → ' + DESTINATION_AIRPORT;

-------------------------------------------------
-- 3️Airline KPI View
-------------------------------------------------
USE Airline_Performance_Analysis;


SELECT * 
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_NAME = 'vw_flights_full';


USE Airline_Performance_Analysis;


-- Drop if exists
DROP VIEW IF EXISTS dbo.vw_flights_full;


-- Create the view
CREATE VIEW dbo.vw_flights_full AS
SELECT f.*, 
       a1.AIRPORT AS ORIGIN_AIRPORT_NAME, a1.CITY AS ORIGIN_CITY, a1.STATE AS ORIGIN_STATE,
       a2.AIRPORT AS DEST_AIRPORT_NAME, a2.CITY AS DEST_CITY, a2.STATE AS DEST_STATE,
       al.AIRLINE AS AIRLINE_NAME
FROM dbo.flights_raw f
LEFT JOIN dbo.airports a1 ON f.ORIGIN_AIRPORT = a1.IATA_CODE
LEFT JOIN dbo.airports a2 ON f.DESTINATION_AIRPORT = a2.IATA_CODE
LEFT JOIN dbo.airlines al ON f.AIRLINE = al.IATA_CODE;


-- Verify
SELECT TOP 5 * FROM dbo.vw_flights_full;


-------------------------------------------------
-- 4️ Airport KPI View (Origin)
-------------------------------------------------
CREATE OR ALTER VIEW dbo.vw_origin_airport_kpis AS
SELECT
    ORIGIN_AIRPORT_NAME,
    COUNT(*) AS Total_Flights,
    SUM(CAST(CANCELLED AS INT))*1.0/COUNT(*) AS Cancellation_Rate,
    SUM(CASE WHEN ARRIVAL_DELAY <= 15 AND CANCELLED = 0 THEN 1 ELSE 0 END)*1.0/COUNT(*) AS OTP_Rate,
    AVG(DEPARTURE_DELAY) AS Avg_Departure_Delay,
    AVG(ARRIVAL_DELAY) AS Avg_Arrival_Delay
FROM dbo.vw_flights_full
GROUP BY ORIGIN_AIRPORT_NAME;

SELECT TOP 10 * 
FROM dbo.vw_origin_airport_kpis
ORDER BY Total_Flights DESC;

-------------------------------------------------
-- 5️ Airport KPI View (Destination)
-------------------------------------------------
CREATE OR ALTER VIEW dbo.vw_dest_airport_kpis AS
SELECT
    DEST_AIRPORT_NAME,
    COUNT(*) AS Total_Flights,
    SUM(CAST(CANCELLED AS INT))*1.0/COUNT(*) AS Cancellation_Rate,
    SUM(CASE WHEN ARRIVAL_DELAY <= 15 AND CANCELLED = 0 THEN 1 ELSE 0 END)*1.0/COUNT(*) AS OTP_Rate,
    AVG(DEPARTURE_DELAY) AS Avg_Departure_Delay,
    AVG(ARRIVAL_DELAY) AS Avg_Arrival_Delay
FROM dbo.vw_flights_full
GROUP BY DEST_AIRPORT_NAME;

SELECT TOP 10 * 
FROM dbo.vw_dest_airport_kpis
ORDER BY Total_Flights DESC;


-------------------------------------------------
-- 6️ Monthly KPI Trends
-------------------------------------------------
CREATE OR ALTER VIEW dbo.vw_monthly_trends AS
SELECT
    MONTH,
    COUNT(*) AS Total_Flights,
    SUM(CAST(CANCELLED AS INT))*1.0/COUNT(*) AS Cancellation_Rate,
    AVG(DEPARTURE_DELAY) AS Avg_Departure_Delay,
    AVG(ARRIVAL_DELAY) AS Avg_Arrival_Delay
FROM dbo.vw_flights_full
GROUP BY MONTH;

SELECT *
FROM dbo.vw_monthly_trends
ORDER BY MONTH;

-------------------------------------------------
-- 7️ Day-of-Week KPI Trends
-------------------------------------------------
CREATE OR ALTER VIEW dbo.vw_dayofweek_trends AS
SELECT
    DAY_OF_WEEK,
    COUNT(*) AS Total_Flights,
    SUM(CAST(CANCELLED AS INT))*1.0/COUNT(*) AS Cancellation_Rate,
    AVG(DEPARTURE_DELAY) AS Avg_Departure_Delay,
    AVG(ARRIVAL_DELAY) AS Avg_Arrival_Delay
FROM dbo.vw_flights_full
GROUP BY DAY_OF_WEEK;

SELECT *
FROM dbo.vw_dayofweek_trends
ORDER BY DAY_OF_WEEK;

-------------------------------------------------
-- 8️ Part-of-Day KPI Trends
-------------------------------------------------
CREATE OR ALTER VIEW dbo.vw_partofday_trends AS
SELECT
    PART_OF_DAY,
    COUNT(*) AS Total_Flights,
    SUM(CAST(CANCELLED AS INT)) * 1.0 / COUNT(*) AS Cancellation_Rate,
    AVG(DEPARTURE_DELAY) AS Avg_Departure_Delay,
    AVG(ARRIVAL_DELAY) AS Avg_Arrival_Delay
FROM dbo.vw_flights_full
GROUP BY PART_OF_DAY;

SELECT *
FROM dbo.vw_partofday_trends 




--Phase 4: Dashboard Development (Power BI / Tableau)







