/*
=====================
Creates database for data warehouse.
Drops database if exists

=====================
*/


USE master;

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')   -- Drops DB if exists within the server.
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE; 
	DROP DATABASE DataWarehouse;
END;

GO

-- Set Single User mode ensures that all other connections will be closed and only current user can access the DB. Mult-user mode is by default which allows multiple users to access the database.
-- ROLLBACK IMMEDIATE cancels all other existing connections to the DB.
-- We then drop the database once all connections are terminated.

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

