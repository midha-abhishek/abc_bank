-- Creating an external file format for parquet files

CREATE EXTERNAL FILE FORMAT [ext_parquet_format]
WITH (
    FORMAT_TYPE = PARQUET
);



-- Creating external data sources

CREATE EXTERNAL DATA SOURCE [silver_extdatasource]
    WITH (LOCATION = 'abfss://silver@dlsaabcbank.dfs.core.windows.net/');

CREATE EXTERNAL DATA SOURCE [gold_extdatasource]
    WITH (LOCATION = 'abfss://gold@dlsaabcbank.dfs.core.windows.net/');



-- Creating customers external table

CREATE EXTERNAL TABLE [customers]
(
    [customer_id] INT,
    [first_name] NVARCHAR(4000),
    [last_name] NVARCHAR(4000),
    [address] NVARCHAR(4000),
    [city] NVARCHAR(4000),
    [state] NVARCHAR(4000),
    [zip] NVARCHAR(4000),
    [ingestion_timestamp] DATETIME
)
WITH (
    LOCATION = '/delta/customers/*.parquet',
    DATA_SOURCE = [silver_extdatasource],
    FILE_FORMAT = [ext_parquet_format]
);



-- Creating accounts external table

CREATE EXTERNAL TABLE [accounts]
(
    [account_id] INT,
    [customer_id] INT,
    [account_type] NVARCHAR(4000),
    [balance] DECIMAL(18, 2),
    [ingestion_timestamp] DATETIME
)
WITH (
    LOCATION = '/delta/accounts/*.parquet',
    DATA_SOURCE = [silver_extdatasource],
    FILE_FORMAT = [ext_parquet_format]
);



-- Creating transactions external table

CREATE EXTERNAL TABLE [transactions]
(
    [transaction_id] INT,
    [account_id] INT,
    [transaction_date] DATE,
    [transaction_amount] DECIMAL(18, 2),
    [transaction_type] NVARCHAR(4000),
    [ingestion_timestamp] DATETIME
)
WITH (
    LOCATION = '/delta/transactions/*.parquet',
    DATA_SOURCE = [silver_extdatasource],
    FILE_FORMAT = [ext_parquet_format]
);



-- Creating loans external table

CREATE EXTERNAL TABLE [loans]
(
    [loan_id] INT,
    [customer_id] INT,
    [loan_amount] DECIMAL(18, 2),
    [interest_rate] DECIMAL(3, 1),
    [loan_term] INT,
    [ingestion_timestamp] DATETIME
)
WITH (
    LOCATION = '/delta/loans/*.parquet',
    DATA_SOURCE = [silver_extdatasource],
    FILE_FORMAT = [ext_parquet_format]
);



-- Creating loan_payments external table

CREATE EXTERNAL TABLE [loan_payments]
(
    [payment_id] INT,
    [loan_id] INT,
    [payment_date] DATE,
    [payment_amount] DECIMAL(18, 2),
    [ingestion_timestamp] DATETIME
)
WITH (
    LOCATION = '/delta/loan_payments/*.parquet',
    DATA_SOURCE = [silver_extdatasource],
    FILE_FORMAT = [ext_parquet_format]
);



-- Creating accounts_customers external table

CREATE EXTERNAL TABLE [accounts_customers]
(
    [account_id] INT,
    [customer_id] INT,
    [first_name] NVARCHAR(4000),
    [last_name] NVARCHAR(4000),
    [balance] DECIMAL(18, 2)
)
WITH (
    LOCATION = '/delta/accounts_customers/*.parquet',
    DATA_SOURCE = [gold_extdatasource],
    FILE_FORMAT = [ext_parquet_format]
);



-- Creating final external table

CREATE EXTERNAL TABLE [final]
(
    [customer_id] INT,
    [first_name] NVARCHAR(4000),
    [last_name] NVARCHAR(4000),
    [address] NVARCHAR(4000),
    [city] NVARCHAR(4000),
    [state] NVARCHAR(4000),
    [zip] NVARCHAR(4000),
    [account_id] INT,
    [account_type] NVARCHAR(4000),
    [balance] DECIMAL(18, 2),
    [total_balance] DECIMAL(18, 2)
)
WITH (
    LOCATION = '/delta/final/*.parquet',
    DATA_SOURCE = [gold_extdatasource],
    FILE_FORMAT = [ext_parquet_format]
);



-- Querieng external tables

SELECT * FROM [customers];

SELECT * FROM [accounts];

SELECT * FROM [transactions];

SELECT * FROM [loans];

SELECT * FROM [loan_payments];

SELECT * FROM [accounts_customers];

SELECT * FROM [final];