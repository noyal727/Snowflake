#Author : Noyal Jonnalagadda

#CSV File Format creation
USE DATABASE SSTRAINING;
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE
  --parse_header = TRUE;
  
  
#S3 storage integration i.e. mounting with Snowflake(Bringing Data to Staging Layer)
CREATE OR REPLACE STORAGE INTEGRATION s3_stage_demo
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::383493188589:role/aashvik-snowflake'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ssttrainingdata/');
DESC INTEGRATION s3_stage_demo;

USE DATABASE SSTRAINING;
USE SCHEMA SSTRAINING;
CREATE OR REPLACE STAGE my_s3_stage
  STORAGE_INTEGRATION = s3_stage_demo
  URL = 's3://ssttrainingdata/'
 FILE_FORMAT = my_csv_format;

ls @my_s3_stage;



#Staging Layer Table Creation
CREATE DATABASE SSTRAINING;
CREATE SCHEMA SSTRAINING;
CREATE SCHEMA SSTRAINING_STAGES;

SELECT * FROM TABLE(
INFER_SCHEMA(
LOCATION=>'@my_s3_stage/products.csv',
FILE_FORMAT=>'my_csv_format'));

USE SCHEMA SSTRAINING;
SELECT GENERATE_COLUMN_DESCRIPTION(ARRAY_AGG(object_construct(*)),'table') AS COLUMNS
FROM TABLE(
INFER_SCHEMA(
LOCATION=>'@my_s3_stage/dimdates.csv',
FILE_FORMAT=>'my_csv_format'));

USE SCHEMA SSTRAINING;
CREATE OR REPLACE TABLE customers(
"CUSTOMER_ID" TEXT,
"CUSTOMER_NAME" TEXT,
"DATE_OF_BIRTH" DATE,
"LOADED_AT" TIMESTAMP_NTZ
)

CREATE OR REPLACE TABLE products(
"PRODUCT_ID" TEXT,
"PRODUCT_NAME" TEXT,
"PRICE" NUMBER(4, 2),
"LOADED_AT" TIMESTAMP_NTZ
)

CREATE OR REPLACE TABLE sales(
"TRANSACTION_ID" NUMBER(5, 0),
"PRODUCT_ID" TEXT,
"CUSTOMER_ID" TEXT,
"STORE_ID" TEXT,
"T_DATE" DATE,
"QUANTITY" NUMBER(2, 0),
"LOADED_AT" TIMESTAMP_NTZ
)

CREATE OR REPLACE TABLE stores(
"STORE_ID" TEXT,
"STORE_NAME" TEXT,
"LOADED_AT" TIMESTAMP_NTZ
)

CREATE OR REPLACE TABLE dimdates(
"Id" NUMBER(6, 0),
"Date" DATE,
"DateLongDescription" TEXT,
"DateShortDescription" TEXT,
"DayLongName" TEXT,
"DayShortName" TEXT,
"MonthLongName" TEXT,
"MonthShortName" TEXT,
"CalendarDay" NUMBER(3, 0),
"CalendarWeek" NUMBER(2, 0),
"CalendarWeekStartDateId" TEXT,
"CalendarWeekEndDateId" TEXT,
"CalendarDayInWeek" NUMBER(1, 0),
"CalendarMonth" NUMBER(2, 0),
"CalendarMonthStartDateId" NUMBER(6, 0),
"CalendarMonthEndDateId" NUMBER(6, 0),
"CalendarNumberOfDaysInMonth" NUMBER(2, 0),
"CalendarDayInMonth" NUMBER(2, 0),
"CalendarQuarter" NUMBER(1, 0),
"CalendarQuarterStartDateId" NUMBER(6, 0),
"CalendarQuarterEndDateId" NUMBER(6, 0),
"CalendarNumberOfDaysInQuarter" NUMBER(2, 0),
"CalendarDayInQuarter" NUMBER(2, 0),
"CalendarYear" NUMBER(4, 0),
"CalendarYearStartDateId" NUMBER(6, 0),
"CalendarYearEndDateId" NUMBER(6, 0),
"CalendarNumberOfDaysInYear" NUMBER(3, 0),
"FiscalDay" NUMBER(1, 0),
"FiscalWeek" NUMBER(1, 0),
"FiscalWeekStartDateId" NUMBER(1, 0),
"FiscalWeekEndDateId" NUMBER(1, 0),
"FiscalDayInWeek" NUMBER(1, 0),
"FiscalMonth" NUMBER(2, 0),
"FiscalMonthStartDateId" NUMBER(6, 0),
"FiscalMonthEndDateId" NUMBER(6, 0),
"FiscalNumberOfDaysInMonth" NUMBER(2, 0),
"FiscalDayInMonth" NUMBER(2, 0),
"FiscalQuarter" NUMBER(1, 0),
"FiscalQuarterStartDateId" NUMBER(6, 0),
"FiscalQuarterEndDateId" NUMBER(6, 0),
"FiscalNumberOfDaysInQuarter" NUMBER(2, 0),
"FiscalDayInQuarter" NUMBER(2, 0),
"FiscalYear" NUMBER(4, 0),
"FiscalYearStartDateId" NUMBER(6, 0),
"FiscalYearEndDateId" NUMBER(6, 0),
"FiscalNumberOfDaysInYear" NUMBER(3, 0),
"LOADED_AT" TIMESTAMP_NTZ
)



#Stored Procedure For Inserting Data Into Staging Layer Tables
USE DATABASE SSTRAINING;
USE SCHEMA SSTRAINING;
CREATE OR REPLACE PROCEDURE update_staging_tables()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    COPY INTO customers(CUSTOMER_ID, CUSTOMER_NAME, DATE_OF_BIRTH, LOADED_AT)
    FROM(
    SELECT $1, $2, $3,CONVERT_TIMEZONE('UTC', 'America/Chicago', CURRENT_TIMESTAMP())
    FROM @my_s3_stage/customers.csv
    )
    FILE_FORMAT = (FORMAT_NAME = my_csv_format);

    DELETE FROM customers
    WHERE LOADED_AT < DATEADD(day, -7, CURRENT_DATE);

    COPY INTO products(PRODUCT_ID,PRODUCT_NAME, PRICE, LOADED_AT)
    FROM(
    SELECT $1, $2, $3,CONVERT_TIMEZONE('UTC', 'America/Chicago', CURRENT_TIMESTAMP())
    FROM @my_s3_stage/products.csv
    )
    FILE_FORMAT = (FORMAT_NAME = my_csv_format);

    DELETE FROM products
    WHERE LOADED_AT < DATEADD(day, -7, CURRENT_DATE);

    
    COPY INTO sales(TRANSACTION_ID, PRODUCT_ID, CUSTOMER_ID, STORE_ID, T_DATE, QUANTITY,LOADED_AT)
    FROM(
    SELECT $1, $2, $3,$4, $5, $6, CONVERT_TIMEZONE('UTC', 'America/Chicago', CURRENT_TIMESTAMP())
    FROM @my_s3_stage/sales.csv
    )
    FILE_FORMAT = (FORMAT_NAME = my_csv_format);

    DELETE FROM sales
    WHERE LOADED_AT < DATEADD(day, -7, CURRENT_DATE);

    
    COPY INTO stores(STORE_ID,STORE_NAME,LOADED_AT)
    FROM(
    SELECT $1, $2,CONVERT_TIMEZONE('UTC', 'America/Chicago', CURRENT_TIMESTAMP())
    FROM @my_s3_stage/stores.csv
    )
    FILE_FORMAT = (FORMAT_NAME = my_csv_format);

    DELETE FROM stores
    WHERE LOADED_AT < DATEADD(day, -7, CURRENT_DATE);
    
    COPY INTO dimdates
    FROM(
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, CONVERT_TIMEZONE('UTC', 'America/Chicago', CURRENT_TIMESTAMP())
    FROM @my_s3_stage/dimdates.csv
    )
    FILE_FORMAT = (FORMAT_NAME = my_csv_format);

    DELETE FROM dimdates
    WHERE LOADED_AT < DATEADD(day, -7, CURRENT_DATE);

END;
$$;

#Scheduled Task Creation to Update Staging Layer Tables using the stored procedure
CREATE OR REPLACE TASK update_staging_tables_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 13 * * * UTC' --7AM US Central Timezone
AS
  CALL update_staging_tables();

USE DATABASE SSTRAINING;
USE SCHEMA SSTRAINING;
ALTER TASK UPDATE_STAGING_TABLES_TASK RESUME;


#Bronze Layer Tables Creation
CREATE TABLE IF NOT EXISTS customers_bronze LIKE customers;

CREATE TABLE IF NOT EXISTS products_bronze LIKE products;

CREATE TABLE IF NOT EXISTS sales_bronze LIKE sales;

CREATE TABLE IF NOT EXISTS stores_bronze LIKE stores;

CREATE TABLE IF NOT EXISTS dimdates_bronze LIKE dimdates;


#Stored Procedure For Inserting Data Into Bronze Layer Tables
USE DATABASE SSTRAINING;
USE SCHEMA SSTRAINING;
CREATE OR REPLACE PROCEDURE update_bronze_tables()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO customers_bronze
    SELECT c.customer_id, c.customer_name, c.date_of_birth, MAX(c.LOADED_AT) AS loaded_at
    FROM customers c
    WHERE DATE(LOADED_AT) = (SELECT MAX(DATE(LOADED_AT)) FROM customers) AND NOT EXISTS (
        SELECT 1
        FROM customers_bronze cb
        WHERE cb.customer_id = c.customer_id
        AND cb.customer_name = c.customer_name
        AND cb.date_of_birth = c.date_of_birth
        AND cb.loaded_at = c.loaded_at
    )
    GROUP BY c.customer_id, c.customer_name, c.date_of_birth;
    
    DELETE FROM customers_bronze
    WHERE DATE(LOADED_AT) <> (SELECT MAX(DATE(LOADED_AT)) FROM customers);
    
    INSERT INTO products_bronze
    SELECT p.product_id, p.product_name, p.price, MAX(p.LOADED_AT) AS loaded_at
    FROM products p
    WHERE DATE(LOADED_AT) = (SELECT MAX(DATE(LOADED_AT)) FROM products) AND NOT EXISTS(
        SELECT 1
        FROM products_bronze pb
        WHERE pb.product_id = p.product_id
        AND pb.product_name = p.product_name
        AND pb.price = p.price
        AND pb.loaded_at = p.loaded_at
    )
    GROUP BY p.product_id, p.product_name, p.price;
    
    DELETE FROM products_bronze
    WHERE DATE(LOADED_AT) <> (SELECT MAX(DATE(LOADED_AT)) FROM products);
    
    INSERT INTO sales_bronze
    SELECT sa.transaction_id, sa.product_id, sa.customer_id, sa.store_id, sa.t_date, sa.quantity, MAX(sa.LOADED_AT) AS loaded_at
    FROM sales sa
    WHERE DATE(LOADED_AT) = (SELECT MAX(DATE(LOADED_AT)) FROM sales) AND NOT EXISTS(
        SELECT 1
        FROM sales_bronze sb
        WHERE sb.transaction_id = sa.transaction_id
        AND sb.product_id = sa.product_id
        AND sb.customer_id = sa.customer_id
        AND sb.store_id = sa.store_id
        AND sb.t_date = sa.t_date
        AND sb.quantity = sa.quantity
        AND sb.loaded_at = sb.loaded_at
    )
    GROUP BY sa.transaction_id, sa.product_id, sa.customer_id, sa.store_id, sa.t_date, sa.quantity;
    
    DELETE FROM sales_bronze
    WHERE DATE(LOADED_AT) <> (SELECT MAX(DATE(LOADED_AT)) FROM sales) OR customer_id NOT IN (
        SELECT customer_id
        FROM customers_bronze
    );
    
    
    INSERT INTO stores_bronze
    SELECT st.store_id, st.store_name, MAX(st.LOADED_AT) AS loaded_at
    FROM stores st
    WHERE DATE(LOADED_AT) = (SELECT MAX(DATE(LOADED_AT)) FROM stores) AND NOT EXISTS(
        SELECT 1
        FROM stores_bronze stb
        WHERE stb.store_id = st.store_id
        AND stb.store_name = st.store_name
        AND stb.loaded_at = st.loaded_at
    )
    GROUP BY st.store_id, st.store_name;
    
    DELETE FROM stores_bronze
    WHERE DATE(LOADED_AT) <> (SELECT MAX(DATE(LOADED_AT)) FROM stores);
    
    INSERT INTO dimdates_bronze
    SELECT * FROM dimdates
    WHERE DATE(LOADED_AT) = (SELECT MAX(DATE(LOADED_AT)) FROM dimdates);
    
    
    DELETE FROM dimdates_bronze
    WHERE DATE(LOADED_AT) <> (SELECT MAX(DATE(LOADED_AT)) FROM dimdates);

END;
$$;

#Scheduled Task Creation to Update Bronze Layer Tables using the stored procedure
CREATE OR REPLACE TASK update_bronze_tables_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 14 * * * UTC' #8AM US Central Timezone
AS
  CALL update_bronze_tables();

USE DATABASE SSTRAINING;
USE SCHEMA SSTRAINING;
#By default tasks won't run even if we created them, so we have to trigger them using following command to start the task
ALTER TASK UPDATE_BRONZE_TABLES_TASK RESUME;

#Silver Layer Tables Creation
-- Sequence for Customers SCD Surrogate Key
CREATE SEQUENCE customer_scd_seq;

CREATE OR REPLACE TABLE customers_scd(
  "SURROGATE_KEY" NUMBER PRIMARY KEY DEFAULT customer_scd_seq.nextval,
  "CUSTOMER_ID" TEXT ,
  "CUSTOMER_NAME" TEXT,
  "DATE_OF_BIRTH" DATE,
  "start_time" TIMESTAMP_NTZ,
  "end_time" TIMESTAMP_NTZ,
  "current_flag_status" STRING
)

-- Sequence for Products SCD Surrogate Key
CREATE SEQUENCE product_scd_seq;

CREATE OR REPLACE TABLE products_scd(
  "SURROGATE_KEY" NUMBER PRIMARY KEY DEFAULT product_scd_seq.nextval,
  "PRODUCT_ID" TEXT,
  "PRODUCT_NAME" TEXT,
  "PRICE" NUMBER(4, 2),
  "start_time" TIMESTAMP_NTZ,
  "end_time" TIMESTAMP_NTZ,
  "current_flag_status" STRING
)

-- Sequence for Sales SCD Surrogate Key
CREATE SEQUENCE sales_scd_seq;

-- Updated Sales SCD Table Definition
CREATE OR REPLACE TABLE sales_scd(
  "SURROGATE_KEY" NUMBER PRIMARY KEY DEFAULT sales_scd_seq.nextval,
  "TRANSACTION_ID" NUMBER(5, 0),
  "PRODUCT_SURROGATE_KEY" NUMBER,
  "CUSTOMER_SURROGATE_KEY" NUMBER,
  "STORE_SURROGATE_KEY" NUMBER,
  "T_DATE" DATE,
  "QUANTITY" NUMBER(2, 0),
  "start_time" TIMESTAMP_NTZ,
  "end_time" TIMESTAMP_NTZ,
  "current_flag_status" STRING,
  FOREIGN KEY ("PRODUCT_SURROGATE_KEY") REFERENCES products_scd("SURROGATE_KEY"),
  FOREIGN KEY ("CUSTOMER_SURROGATE_KEY") REFERENCES customers_scd("SURROGATE_KEY"),
  FOREIGN KEY ("STORE_SURROGATE_KEY") REFERENCES stores_scd("SURROGATE_KEY")
);


-- Sequence for Stores SCD Surrogate Key
CREATE SEQUENCE store_scd_seq;

CREATE OR REPLACE TABLE stores_scd(
  "SURROGATE_KEY" NUMBER PRIMARY KEY DEFAULT store_scd_seq.nextval,
  "STORE_ID" TEXT,
  "STORE_NAME" TEXT,
  "start_time" TIMESTAMP_NTZ,
  "end_time" TIMESTAMP_NTZ,
  "current_flag_status" STRING
)


#Streams creation
CREATE or REPLACE STREAM customers_check on table customers_bronze;

CREATE OR REPLACE STREAM products_check on table products_bronze;

CREATE OR REPLACE STREAM sales_check on table sales_bronze;

CREATE OR REPLACE STREAM stores_check on table stores_bronze;

CREATE OR REPLACE STREAM dimdates_check on table dimdates_bronze;


#Stored Procedure For Inserting Data Into Silver Layer Tables
USE DATABASE SSTRAINING;
USE SCHEMA SSTRAINING;
CREATE OR REPLACE PROCEDURE update_silver_tables()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    #Pushing data into Silver layer tables based on constraints
    MERGE INTO customers_scd cscd
    USING (SELECT * from customers_check) cchk
    ON cscd.customer_id = cchk.customer_id
    AND cscd.date_of_birth = cchk.date_of_birth
    -- Handling updates or setting inactive for deleted records
    WHEN MATCHED AND (cchk.metadata$action = 'DELETE')
    THEN 
    UPDATE SET "end_time" = CONVERT_TIMEZONE('UTC', 'America/Chicago', CURRENT_TIMESTAMP()),
    "current_flag_status" = 'False'
    -- Handling reactivation of previously inactive records
    WHEN MATCHED AND "current_flag_status" = 'False' AND (cchk.metadata$action = 'INSERT') THEN 
    UPDATE SET 
    "start_time" = CONVERT_TIMEZONE('UTC', 'America/Chicago', CURRENT_TIMESTAMP()),
    "end_time" = '9999-12-31',
    "current_flag_status" = 'True'
    -- Handling new records inserts
    WHEN NOT MATCHED AND (cchk.metadata$action='INSERT')
    THEN
    INSERT("CUSTOMER_ID", "CUSTOMER_NAME", "DATE_OF_BIRTH", "start_time", "end_time", "current_flag_status")
    VALUES(cchk.customer_id, cchk.customer_name, cchk.date_of_birth, CONVERT_TIMEZONE('UTC', 'America/Chicago', CURRENT_TIMESTAMP()), '9999-12-31', 'True' );
    
    
    
    MERGE INTO products_scd pscd
    USING (select * FROM products_check) pchk
    ON pscd.product_id = pchk.product_id
    AND pscd.price = pscd.price
    -- Handling updates or setting inactive for deleted records
    WHEN MATCHED AND (pchk.metadata$action = 'DELETE')
    THEN 
    UPDATE SET "end_time" = CONVERT_TIMEZONE('UTC', 'America/Chicago', CURRENT_TIMESTAMP()),
    "current_flag_status" = 'False'
    -- Handling reactivation of previously inactive records
    WHEN MATCHED AND "current_flag_status" = 'False' AND (pchk.metadata$action = 'INSERT') THEN 
    UPDATE SET 
    "start_time" = CONVERT_TIMEZONE('UTC', 'America/Chicago', CURRENT_TIMESTAMP()),
    "end_time" = '9999-12-31',
    "current_flag_status" = 'True'
    -- Handling new records inserts
    WHEN NOT MATCHED AND (pchk.metadata$action='INSERT')
    THEN
    INSERT("PRODUCT_ID", "PRODUCT_NAME", "PRICE", "start_time", "end_time", "current_flag_status")
    VALUES(pchk.product_id, pchk.product_name, pchk.price, CONVERT_TIMEZONE('UTC', 'America/Chicago', CURRENT_TIMESTAMP()), '9999-12-31', 'True' );
    
    
    
    MERGE INTO stores_scd stscd
    USING (SELECT * FROM stores_check) stchk
    ON stscd.store_id = stchk.store_id
    -- Handling updates or setting inactive for deleted records
    WHEN MATCHED AND (stchk.metadata$action = 'DELETE')
    THEN 
    UPDATE SET "end_time" = CONVERT_TIMEZONE('UTC', 'America/Chicago', CURRENT_TIMESTAMP()),
    "current_flag_status" = 'False'
    -- Handling reactivation of previously inactive records
    WHEN MATCHED AND "current_flag_status" = 'False' AND (stchk.metadata$action = 'INSERT') THEN 
    UPDATE SET 
    "start_time" = CONVERT_TIMEZONE('UTC', 'America/Chicago', CURRENT_TIMESTAMP()),
    "end_time" = '9999-12-31',
    "current_flag_status" = 'True'
    -- Handling new records inserts
    WHEN NOT MATCHED AND (stchk.metadata$action='INSERT')
    THEN
    INSERT("STORE_ID", "STORE_NAME",  "start_time", "end_time", "current_flag_status")
    VALUES(stchk.store_id, stchk.store_name, CONVERT_TIMEZONE('UTC', 'America/Chicago', CURRENT_TIMESTAMP()), '9999-12-31', 'True' );
    
    
    
    MERGE INTO sales_scd target
    USING (
        SELECT 
            s.TRANSACTION_ID,
            s.T_DATE,
            s.QUANTITY,
            c.SURROGATE_KEY AS CUSTOMER_SURROGATE_KEY,
            p.SURROGATE_KEY AS PRODUCT_SURROGATE_KEY,
            st.SURROGATE_KEY AS STORE_SURROGATE_KEY,
            s.metadata$action
        FROM sales_check s
        JOIN customers_scd c ON s.CUSTOMER_ID = c.CUSTOMER_ID AND c."current_flag_status" = 'True'
        JOIN products_scd p ON s.PRODUCT_ID = p.PRODUCT_ID AND p."current_flag_status" = 'True'
        JOIN stores_scd st ON s.STORE_ID = st.STORE_ID AND st."current_flag_status" = 'True'
    ) source
    ON target.TRANSACTION_ID = source.TRANSACTION_ID
    WHEN MATCHED AND source.metadata$action = 'DELETE' THEN 
        UPDATE SET target."end_time" = CURRENT_TIMESTAMP(), target."current_flag_status" = 'False'
    WHEN MATCHED AND target."current_flag_status" = 'False' AND source.metadata$action = 'INSERT' THEN 
        UPDATE SET target."start_time" = CURRENT_TIMESTAMP(), target."end_time" = '9999-12-31', target."current_flag_status" = 'True'
    WHEN NOT MATCHED AND source.metadata$action = 'INSERT' THEN 
        INSERT ("TRANSACTION_ID", "PRODUCT_SURROGATE_KEY", "CUSTOMER_SURROGATE_KEY", "STORE_SURROGATE_KEY", "T_DATE", "QUANTITY", "start_time", "end_time", "current_flag_status")
        VALUES (source.TRANSACTION_ID, source.PRODUCT_SURROGATE_KEY, source.CUSTOMER_SURROGATE_KEY, source.STORE_SURROGATE_KEY, source.T_DATE, source.QUANTITY, CURRENT_TIMESTAMP(), '9999-12-31', 'True');


END;
$$;


#Scheduled Task Creation to Update Silver Layer Tables using the stored procedure
CREATE OR REPLACE TASK update_silver_tables_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 15 * * * UTC' -- 9AM US Central Timezone
as 
CALL update_silver_tables();

USE DATABASE SSTRAINING;
USE SCHEMA SSTRAINING;
ALTER TASK UPDATE_SILVER_TABLES_TASK RESUME;
