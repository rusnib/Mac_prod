cas casauto;
caslib _all_ assign;

%let lmvResultTableDec = MAX_CASL.PMIX_DAYS_RESULT_DEC;
%let lmvResultTableJan = MAX_CASL.PMIX_DAYS_RESULT_JAN;
%let lmvResultTableMar = MAX_CASL.PMIX_DAYS_RESULT_MAR;

%let lmvPlanTableDec = CASUSER.FCST_UNITS_DEC;
%let lmvPlanTableJan = CASUSER.FCST_UNITS_JAN;
%let lmvPlanTableMar = CASUSER.FCST_UNITS_MAR;


proc fedsql sessref=casauto;
	create table CASUSER.FCST_UNITS_DEC{options replace=true} as
	select 
		  main.product_id
		, sku.PRODUCT_NM
		, case 
			when sku.product_id = sku.regular_id or  sku.regular_id is null
				then 0
			else 1
		  end as promo_sku
		, main.pbo_location_id
		, pbo.PBO_LOCATION_NM
		, main.sales_dt
		, main.FINAL_FCST_UNITS_ML
		, main.FINAL_FCST_UNITS_REC_BPLM
		, main.FINAL_FCST_UNITS_REC_APLM
		, pr.price_net * main.FINAL_FCST_UNITS_ML       as FINAL_FCST_SALE_ML 
		, pr.price_net * main.FINAL_FCST_UNITS_REC_BPLM as FINAL_FCST_SALE_REC_BPLM
		, pr.price_net * main.FINAL_FCST_UNITS_REC_APLM as FINAL_FCST_SALE_REC_APLM
		, pr.price_net
		, pr.price_net_curr

	from MAX_CASL.FCST_UNITS_DEC as main
		  
	left join MAX_CASL.KPI_prices 	as pr	
		on  main.product_id			= pr.product_id 
		and main.pbo_location_id	= pr.pbo_location_id 
		and main.sales_dt			= pr.period_dt

	left join CASUSER.PBO_DICTIONARY 	as 	pbo
		on  main.pbo_location_id	= pbo.pbo_location_id 

	left join CASUSER.PRODUCT_DICTIONARY 	as sku	
		on  main.product_id			= sku.product_id 

	;
	create table CASUSER.FCST_UNITS_JAN{options replace=true} as
	select 
		   main.product_id
		, sku.PRODUCT_NM
		, case 
			when sku.product_id = sku.regular_id or  sku.regular_id is null
				then 0
			else 1
		  end as promo_sku
		, main.pbo_location_id
		, pbo.PBO_LOCATION_NM
		, main.sales_dt
		, main.FINAL_FCST_UNITS_ML
		, main.FINAL_FCST_UNITS_REC_BPLM
		, main.FINAL_FCST_UNITS_REC_APLM
		, pr.price_net * main.FINAL_FCST_UNITS_ML       as FINAL_FCST_SALE_ML 
		, pr.price_net * main.FINAL_FCST_UNITS_REC_BPLM as FINAL_FCST_SALE_REC_BPLM
		, pr.price_net * main.FINAL_FCST_UNITS_REC_APLM as FINAL_FCST_SALE_REC_APLM
		, pr.price_net
		, pr.price_net_curr

	from MAX_CASL.FCST_UNITS_JAN as main
		  
	left join MAX_CASL.KPI_prices 	as pr	
		on  main.product_id			= pr.product_id 
		and main.pbo_location_id	= pr.pbo_location_id 
		and main.sales_dt			= pr.period_dt
	
	left join CASUSER.PBO_DICTIONARY 	as 	pbo
		on  main.pbo_location_id	= pbo.pbo_location_id 

	left join CASUSER.PRODUCT_DICTIONARY 	as sku	
		on  main.product_id			= sku.product_id 

	;
	create table CASUSER.FCST_UNITS_MAR{options replace=true} as
	select 
		   main.product_id
		, sku.PRODUCT_NM
		, case 
			when sku.product_id = sku.regular_id or  sku.regular_id is null
				then 0
			else 1
		  end as promo_sku
		, main.pbo_location_id
		, pbo.PBO_LOCATION_NM
		, main.sales_dt
		, main.FINAL_FCST_UNITS_ML
		, main.FINAL_FCST_UNITS_REC_BPLM
		, main.FINAL_FCST_UNITS_REC_APLM
		, pr.price_net * main.FINAL_FCST_UNITS_ML       as FINAL_FCST_SALE_ML 
		, pr.price_net * main.FINAL_FCST_UNITS_REC_BPLM as FINAL_FCST_SALE_REC_BPLM
		, pr.price_net * main.FINAL_FCST_UNITS_REC_APLM as FINAL_FCST_SALE_REC_APLM
		, pr.price_net
		, pr.price_net_curr

	from MAX_CASL.FCST_UNITS_MAR as main
		  
	left join MAX_CASL.KPI_prices 	as pr	
		on  main.product_id			= pr.product_id 
		and main.pbo_location_id	= pr.pbo_location_id 
		and main.sales_dt			= pr.period_dt
	
	left join CASUSER.PBO_DICTIONARY 	as 	pbo
		on  main.pbo_location_id	= pbo.pbo_location_id 

	left join CASUSER.PRODUCT_DICTIONARY 	as sku	
		on  main.product_id			= sku.product_id 
	;
quit;


/* Сумма до SKU по всем ПБО */
proc fedsql sessref=casauto;
	create table casuser.data_hist{options replace=true} as
	select product_id, sales_dt
		, sum(coalesce(sales_qty, 0) + coalesce(sales_qty_promo,0)) as sum_qty
		, sum(net_sales_amt) as sum_rur
	from MN_SHORT.PMIX_SALES
	where channel_cd = 'ALL'
	group by product_id, sales_dt
;
quit;

data casuser.data_hist;
	set casuser.data_hist;
 	group = "ACTUAL";
run;

/* DECEMBER */
proc fedsql sessref=casauto;
	create table casuser.data_ml_dec{options replace=true} as
	select product_id, sales_dt
		, sum(FINAL_FCST_UNITS_ML) as sum_qty
		, sum(FINAL_FCST_SALE_ML) as sum_rur
	from &lmvPlanTableDec.
	group by 1,2
;
quit;

data casuser.data_ml_dec;
	set casuser.data_ml_dec;
 	group = "ML_DEC";
run;

proc fedsql sessref=casauto;
	create table casuser.data_BR_dec{options replace=true} as
	select 
		  product_id, sales_dt
		, sum(FINAL_FCST_UNITS_REC_BPLM) as sum_qty
		, sum(FINAL_FCST_SALE_REC_BPLM) as sum_rur
	from &lmvPlanTableDec.
	group by 1,2
;
quit;

data casuser.data_BR_dec;
	set casuser.data_BR_dec;
 	group = "BR_DEC";
run;

proc fedsql sessref=casauto;
	create table casuser.data_AR_dec{options replace=true} as
	select 
		  product_id, sales_dt
		, sum(FINAL_FCST_UNITS_REC_APLM) as sum_qty
		, sum(FINAL_FCST_SALE_REC_APLM) as sum_rur
	from &lmvPlanTableDec.
	group by 1,2
;
quit;

data casuser.data_AR_dec;
	set casuser.data_AR_dec;
 	group = "AR_DEC";
run;

proc fedsql sessref=casauto;
	create table casuser.data_plan_dec{options replace=true} as
	select 
		  product_id, period_dt as sales_dt
		, sum(FINAL_FCST_UNITS) as sum_qty
		, sum(FINAL_FCST_SALE) as sum_rur
	from MAX_CASL.FCST_UNITS_N_SALE_DEC
	group by 1,2
;
quit;

data casuser.data_plan_dec;
	set casuser.data_plan_dec;
 	group = "DP_DEC";
run;



/* JANUARY */
proc fedsql sessref=casauto;
	create table casuser.data_ml_JAN{options replace=true} as
	select product_id, sales_dt
		, sum(FINAL_FCST_UNITS_ML) as sum_qty
		, sum(FINAL_FCST_SALE_ML) as sum_rur
	from &lmvPlanTableJAN.
	group by 1,2
;
quit;

data casuser.data_ml_JAN;
	set casuser.data_ml_JAN;
 	group = "ML_JAN";
run;

proc fedsql sessref=casauto;
	create table casuser.data_BR_JAN{options replace=true} as
	select 
		  product_id, sales_dt
		, sum(FINAL_FCST_UNITS_REC_BPLM) as sum_qty
		, sum(FINAL_FCST_SALE_REC_BPLM) as sum_rur
	from &lmvPlanTableJAN.
	group by 1,2
;
quit;

data casuser.data_BR_JAN;
	set casuser.data_BR_JAN;
 	group = "BR_JAN";
run;

proc fedsql sessref=casauto;
	create table casuser.data_AR_JAN{options replace=true} as
	select 
		  product_id, sales_dt
		, sum(FINAL_FCST_UNITS_REC_APLM) as sum_qty
		, sum(FINAL_FCST_SALE_REC_APLM) as sum_rur
	from &lmvPlanTableJAN.
	group by 1,2
;
quit;

data casuser.data_AR_JAN;
	set casuser.data_AR_JAN;
 	group = "AR_JAN";
run;

proc fedsql sessref=casauto;
	create table casuser.data_plan_JAN{options replace=true} as
	select 
		  product_id, period_dt as sales_dt
		, sum(FINAL_FCST_UNITS) as sum_qty
		, sum(FINAL_FCST_SALE) as sum_rur
	from MAX_CASL.FCST_UNITS_N_SALE_JAN
	group by 1,2
;
quit;

data casuser.data_plan_JAN;
	set casuser.data_plan_JAN;
 	group = "DP_JAN";
run;


/* MARCH */
proc fedsql sessref=casauto;
	create table casuser.data_ml_MAR{options replace=true} as
	select product_id, sales_dt
		, sum(FINAL_FCST_UNITS_ML) as sum_qty
		, sum(FINAL_FCST_SALE_ML) as sum_rur
	from &lmvPlanTableMAR.
	group by 1,2
;
quit;

data casuser.data_ml_MAR;
	set casuser.data_ml_MAR;
 	group = "ML_MAR";
run;

proc fedsql sessref=casauto;
	create table casuser.data_BR_MAR{options replace=true} as
	select 
		  product_id, sales_dt
		, sum(FINAL_FCST_UNITS_REC_BPLM) as sum_qty
		, sum(FINAL_FCST_SALE_REC_BPLM) as sum_rur
	from &lmvPlanTableMAR.
	group by 1,2
;
quit;

data casuser.data_BR_MAR;
	set casuser.data_BR_MAR;
 	group = "BR_MAR";
run;

proc fedsql sessref=casauto;
	create table casuser.data_AR_MAR{options replace=true} as
	select 
		  product_id, sales_dt
		, sum(FINAL_FCST_UNITS_REC_APLM) as sum_qty
		, sum(FINAL_FCST_SALE_REC_APLM) as sum_rur
	from &lmvPlanTableMAR.
	group by 1,2
;
quit;

data casuser.data_AR_MAR;
	set casuser.data_AR_MAR;
 	group = "AR_MAR";
run;

proc fedsql sessref=casauto;
	create table casuser.data_plan_MAR{options replace=true} as
	select 
		  product_id, period_dt as sales_dt
		, sum(FINAL_FCST_UNITS) as sum_qty
		, sum(FINAL_FCST_SALE) as sum_rur
	from MAX_CASL.FCST_UNITS_N_SALE_MAR
	group by 1,2
;
quit;

data casuser.data_plan_MAR;
	set casuser.data_plan_MAR;
 	group = "DP_MAR";
run;




proc casutil;
	droptable 
		casdata="check_backtest_fcst" 
		incaslib="casuser" 
		quiet
	;
run;

data casuser.check_backtest_fcst;
	set 
		casuser.data_hist
		casuser.data_ml_dec
		casuser.data_br_dec
		casuser.data_ar_dec		
		casuser.data_plan_dec	
		casuser.data_ml_jan
		casuser.data_br_jan
		casuser.data_ar_jan		
		casuser.data_plan_jan	
		casuser.data_ml_mar
		casuser.data_br_mar
		casuser.data_ar_mar		
		casuser.data_plan_mar
	;
run;


proc casutil;
	promote 
		casdata="check_backtest_fcst" 
		casout="check_backtest_fcst"  
		incaslib="casuser" 
		outcaslib="casuser"
	;
run;






