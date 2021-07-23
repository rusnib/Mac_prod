cas casauto;
caslib _all_ assign;

%let lmvResultTableDec = MAX_CASL.PMIX_DAYS_RESULT_DEC;
%let lmvResultTableJan = MAX_CASL.PMIX_DAYS_RESULT_JAN;
%let lmvResultTableMar = MAX_CASL.PMIX_DAYS_RESULT_MAR;

%let lmvPlanTableDec = MAX_CASL.FCST_UNITS_N_SALE_DEC;
%let lmvPlanTableJan = MAX_CASL.FCST_UNITS_N_SALE_JAN;
%let lmvPlanTableMar = MAX_CASL.FCST_UNITS_N_SALE_MAR;


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
	create table casuser.data_fcst_dec{options replace=true} as
	select product_id, sales_dt
		, sum(P_SUM_QTY) as sum_qty
	from &lmvResultTableDec.
	where channel_cd = 1
	group by product_id, sales_dt
;
quit;

data casuser.data_fcst_dec;
	set casuser.data_fcst_dec;
 	group = "ML_DEC";
run;

proc fedsql sessref=casauto;
	create table casuser.data_plan_dec{options replace=true} as
	select 
		  product_id, period_dt as sales_dt
		, sum(FINAL_FCST_UNITS) as sum_qty
		, sum(FINAL_FCST_SALE) as sum_rur
	from &lmvPlanTableDec.
	group by 1,2
;
quit;

data casuser.data_plan_dec;
	set casuser.data_plan_dec;
 	group = "DP_DEC";
run;


/* JANUARY */
proc fedsql sessref=casauto;
	create table casuser.data_fcst_jan{options replace=true} as
	select product_id, sales_dt
		, sum(P_SUM_QTY) as sum_qty
	from &lmvResultTableJan.
	where channel_cd = 1
	group by product_id, sales_dt
;
quit;

data casuser.data_fcst_jan;
	set casuser.data_fcst_jan;
 	group = "ML_JAN";
run;

proc fedsql sessref=casauto;
	create table casuser.data_plan_jan{options replace=true} as
	select 
		  product_id, period_dt as sales_dt
		, sum(FINAL_FCST_UNITS) as sum_qty
		, sum(FINAL_FCST_SALE) as sum_rur
	from &lmvPlanTableJan.
	group by 1,2
;
quit;

data casuser.data_plan_jan;
	set casuser.data_plan_jan;
 	group = "DP_JAN";
run;

/* MARCH */
proc fedsql sessref=casauto;
	create table casuser.data_fcst_mar{options replace=true} as
	select product_id, sales_dt
		, sum(P_SUM_QTY) as sum_qty
	from &lmvResultTableMar.
	where channel_cd = 1
	group by product_id, sales_dt
;
quit;

data casuser.data_fcst_mar;
	set casuser.data_fcst_mar;
 	group = "ML_MAR";
run;


proc fedsql sessref=casauto;
	create table casuser.data_plan_mar{options replace=true} as
	select 
		  product_id, period_dt as sales_dt
		, sum(FINAL_FCST_UNITS) as sum_qty
		, sum(FINAL_FCST_SALE) as sum_rur
	from &lmvPlanTableMAR.
	group by 1,2
;
quit;

data casuser.data_plan_mar;
	set casuser.data_plan_mar;
 	group = "DP_MAR";
run;

/*
proc fedsql sessref=casauto;
	create table casuser.data_wplm{options replace=true} as
	select product_id, period_dt as sales_dt
	, sum(FF) as sum_qty
	from MN_SHORT.FC_W_PLM
	where channel_cd = 'ALL'
	group by product_id, period_dt
;
quit;

data casuser.data_wplm;
	set casuser.data_wplm;
 	group = "2_PLM+NEW";
run;


proc fedsql sessref=casauto;
	create table casuser.data_brec{options replace=true} as
	select product_id, period_dt as sales_dt
	, sum(FF_BEFORE_REC) as sum_qty
	from MN_SHORT.FCST_RECONCILED
	where channel_cd = 'ALL'
	group by product_id, period_dt
;
quit;

data casuser.data_brec;
	set casuser.data_brec;
 	group = "3_BEF_REC";
run;


proc fedsql sessref=casauto;
	create table casuser.data_arec{options replace=true} as
	select product_id, period_dt as sales_dt
	, sum(FF) as sum_qty
	from MN_SHORT.FCST_RECONCILED
	where channel_cd = 'ALL'
	group by product_id, period_dt
;
quit;

data casuser.data_arec;
	set casuser.data_arec;
 	group = "4_AFT_REC";
run;


proc fedsql sessref=casauto;
	create table casuser.data_plan{options replace=true} as
	select 
		  PROD as product_id, data as sales_dt
		, sum(FINAL_FCST_UNITS) as sum_qty
		, sum(FINAL_FCST_SALE) as sum_rur
	from MN_SHORT.PLAN_PMIX_DAY
	group by PROD, data
;
quit;

data casuser.data_plan;
	set casuser.data_plan;
 	group = "5_PLAN_DP";
run;

*/



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
		casuser.data_fcst_dec
		casuser.data_fcst_jan
		casuser.data_fcst_mar
		casuser.data_plan_dec
		casuser.data_plan_jan
		casuser.data_plan_mar
/* 		casuser.data_wplm */
/* 		casuser.data_brec */
/* 		casuser.data_arec */
/* 		casuser.data_plan */
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

/*  */
/* proc fedsql sessref=casauto; */
/* 	create table casuser.data_union_pmix_by_sku {options replace=true} as */
/* 	select  */
/* 		  coalesce(hist.product_id, fcst.product_id) as product_id */
/* 		, coalesce(hist.sales_dt, fcst.sales_dt) as sales_dt */
/* 		, fcst.sum_fcst_qty */
/* 		, hist.sum_hist_qty */
/* 	from casuser.data_hist as hist */
/* 	full join casuser.data_fcst as fcst */
/* 		on hist.product_id = fcst.product_id */
/* 			and hist.sales_dt = fcst.sales_dt */
/* ; */
/* quit; */







