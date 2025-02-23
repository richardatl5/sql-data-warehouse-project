/*
=====================
Bulk Insert
Focus is on data completion and match from source, which currently is a local file.
Tablock is used to lock the table during import.

Validated data count and mapping.

Using truncate to delete the data, it is much faster to delete everything opposed to the delete function which takes longer to delete.
Not using a where clause to delete.
=====================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME
	BEGIN TRY
		--USE DataWarehouse;
		--GO

		-- CRM Cust Info
		PRINT '==========================';
		PRINT 'Loading Bronze Layer';
		PRINT '==========================';


		PRINT '--------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------';
		SET @start_time = GETDATE();

		TRUNCATE TABLE bronze.crm_cust_info; 

			BULK INSERT bronze.crm_cust_info

			FROM 'C:\Users\phili\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'

			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK

			);

		-- CRM Prd Info

		TRUNCATE TABLE bronze.crm_prd_info;

			BULK INSERT bronze.crm_prd_info

			FROM 'C:\Users\phili\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'

			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK

			);

		-- CRM Sales_details

		TRUNCATE TABLE bronze.crm_sales_details;

			BULK INSERT bronze.crm_sales_details

			FROM 'C:\Users\phili\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'

			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK

			);


		PRINT '--------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------';

		-- ERP CUST_AZ12

		TRUNCATE TABLE bronze.erp_cust_az12;

			BULK INSERT bronze.erp_cust_az12

			FROM 'C:\Users\phili\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'

			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK

			);

		-- ERP LOC_A101

		TRUNCATE TABLE bronze.erp_loc_a101;

			BULK INSERT bronze.erp_loc_a101

			FROM 'C:\Users\phili\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'

			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);

		--ERP PX_CAT_G1V2

		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

			BULK INSERT bronze.erp_px_cat_g1v2

			FROM 'C:\Users\phili\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'

			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		
		SET @end_time = GETDATE();
	END TRY

	BEGIN CATCH
		PRINT'=====================';
		PRINT'Loading Bronze Failed';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT'=====================';
	END CATCH

	PRINT'=====================';
	PRINT 'Loading Time is ' +  CAST(DATEDIFF(ms,  @start_time, @end_time) AS NVARCHAR) + ' millseconds'  ;

END
	
--select * from bronze.erp_px_cat_g1v2
