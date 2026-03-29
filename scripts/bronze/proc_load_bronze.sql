/*
============================================
Stored procedure : Load Bronze Layer (Source -> bronze)
==============================================
Script Purpose:
This stored procedure loads the data 'bronze' schema from external CSV files. 
It perform the following actions:
- Turnicate the bronze tables before Loading the date.
- Uses the 'BULK INSERT' command to load the data from CSV files to bronze tables.

Parameter :
NO=one.
This stored procedure does not accept any parameter or return values.

Usage Example :
Exec bronze.load_bronze;
*/
--exec bronze.load_bronze
=========================================
create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime
	begin try
		set @batch_start_time=getdate();
		print '==============================================';
		print 'Loading bronze layer';
		print '===============================================';

		print '==============================================';
		print 'Loading CRM Table';
		print '===============================================';
		set @start_time=GETDATE();
		print '>> truncating the table: bronze.crm_cust_info'
		--truncate table  bronze.crm_cust_info
		print '>> Inserting the data : bronze.crm_cust_info'
		--bulk insert bronze.crm_cust_info
		--from 'C:\Data\cust_info.csv'
		--with (
		--	firstrow=2,
		--	fieldterminator=',',
		--	tablock
		--)
		set @end_time=GETDATE();
		print '>> Load Duration :'+ cast(datediff(second,@start_time,@end_time)as nvarchar)+ 'secods';
		print '------------------------';

		set @start_time=GETDATE();
		print '>> truncating the table: bronze.crm_prd_info'
		truncate table  bronze.crm_prd_info
		print '>> Inserting the data : bronze.crm_prd_info' 
		bulk insert bronze.crm_prd_info 
		from 'C:\Data\prd_info.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		)
		set @end_time=GETDATE();
		print '>> Load Duration :'+ cast(datediff(second,@start_time,@end_time)as nvarchar)+ 'secods';


		set @start_time=GETDATE();
		print '>> truncating the table: bronze.crm_sales_details'
		truncate table  bronze.crm_sales_details
		print '>> Inserting the data : bronze.crm_sales_details' 
		--bulk insert bronze.crm_sales_details 
		--from 'C:\Data\sales_details.csv'
		--with (
		--	firstrow=2,
		--	fieldterminator=',',
		--	tablock
		--)
		set @end_time=GETDATE();
		print '>> Load Duration :'+ cast(datediff(second,@start_time,@end_time)as nvarchar)+ 'secods';
		print '==============================================';
		print 'Loading ERP Table';
		print '===============================================';

		set @start_time=GETDATE();
		print '>> truncating the table: bronze.erp_loc_a101'
		truncate table  bronze.erp_loc_a101
		print '>> Inserting the data : bronze.erp_loc_a101' 
		bulk insert bronze.erp_loc_a101 
		from 'C:\Data\loc_a101.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		)
		set @end_time=GETDATE();
		print '>> Load Duration :'+ cast(datediff(second,@start_time,@end_time)as nvarchar)+ 'secods';

		set @start_time=GETDATE();
		print '>> truncating the table: bronze.erp_cust_az12'
		truncate table  bronze.erp_cust_az12
		print '>> Inserting the data : bronze.erp_cust_az12' 
		bulk insert bronze.erp_cust_az12 
		from 'C:\Data\cust_az12.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		)
		set @end_time=GETDATE();
		print '>> Load Duration :'+ cast(datediff(second,@start_time,@end_time)as nvarchar)+ 'secods';

		set @start_time=GETDATE();
		print '>> truncating the table: bronze.erp_px_cat_glv2'
		truncate table  bronze.erp_px_cat_glv2
		print '>> Inserting the data : bronze.erp_px_cat_glv2' 
		bulk insert bronze.erp_px_cat_glv2 
		from 'C:\Data\px_cat_g1v2px.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
		set @end_time=GETDATE();
		print '>> Load Duration :'+ cast(datediff(second,@start_time,@end_time)as nvarchar)+ 'secods';
		print '----------------------'
		
		set @end_time=GETDATE()
		print 'Loading bronze layer is completed';
		print '  Total Load Duration :'+cast(datediff(second,@batch_start_time, @batch_end_time) as nvarchar)+'seconds';
		print '----------------------------------'
	end try
	begin catch
	print '=================================='
	print 'Error occured during loading bronze layer'
	print 'Error Message'+ error_message();
	print 'Error Number'+ cast(Error_number() as nvarchar);
	print 'Error State'+ cast(Error_state() as nvarchar);
	print '=================================='
	end catch
end
