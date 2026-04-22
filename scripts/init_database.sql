-- ================================================
-- Script: DataWarehouse Database Initialization
-- Purpose: Create a fresh database for DWH project
-- Features: Bronze, Silver, Gold schemas
-- ================================================

-- Switch to master DB context
USE master;
GO

-- Drop and recreate the 'DataWarehouse' database if it already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    -- Force single-user mode to terminate active connections
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    -- Drop the existing database
    DROP DATABASE DataWarehouse;
END;
GO

-- Create a new 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

-- Switch context to the newly created DataWarehouse DB
USE DataWarehouse;
GO

-- ================================================
-- Schema Layering for DWH Project
-- Bronze: Raw data ingestion layer
-- Silver: Cleansed, transformed, and standardized data
-- Gold: Curated, business-ready data for analytics/reporting
-- ================================================

-- Create Bronze schema (raw data)
CREATE SCHEMA bronze;
GO

-- Create Silver schema (cleansed/transformed data)
CREATE SCHEMA silver;
GO

-- Create Gold schema (curated/reporting layer)
CREATE SCHEMA gold;
GO
