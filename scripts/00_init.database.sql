/*
===========================================================
 ⚠️  WARNING BEFORE RUNNING THIS SCRIPT
===========================================================
This script will **DROP and RECREATE** the database named `DataWarehouse`.

➡️ All existing data, tables, and objects inside the `DataWarehouse` 
    database (if it already exists) will be permanently deleted.

✅ Run this script ONLY if:
   - You are setting up a new environment, OR
   - You have a backup of your current database.

❌ Do NOT run this script on a production or live database.

-----------------------------------------------------------
   Safe Practice:
   1. Verify database name before running.
   2. Backup any important data.
   3. Run in a test/development environment.
-----------------------------------------------------------
*/


-- Create Database 'DataWarehouse'
use master;
GO


-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN 
   ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
   DROP DATABASE DataWarehouse;
END;
GO


-- Create the 'DataWarehouse' database
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
