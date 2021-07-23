
cas casauto;
caslib _all_ assign;
    
/* PMIX_DAYS_RESULT (scoring) -> plan_*_month\day */
/*
proc fedsql sessref=casauto;
	create table casuser.test_plan{options replace=true} as
	select count(data) as count_dts
		, sum(FINAL_FCST_UNITS) as final_qty
		, sum(OVERRIDED_FCST_UNITS) as overd_fcst 
		, sum(PROMO_FCST_UNITS) as promo_fcst 
	from MN_SHORT.PLAN_PMIX_DAY
;
quit;

proc fedsql sessref=casauto;
	create table casuser.test_fcst{options replace=true} as
	select channel_cd
		,  count(sales_dt) as count_dts
		, sum(sales_qty) as sales_qty
		, sum(sales_qty_promo) as sales_qty_promo 
	from MN_SHORT.PMIX_SALES
	group by channel_cd
;
quit;

proc fedsql sessref=casauto;
	create table casuser.test_hist{options replace=true} as
	select channel_cd
		,  count(sales_dt) as count_dts
		, sum(p_sum_qty) as p_sum_qty
	from MN_SHORT.PMIX_DAYS_RESULT
	group by channel_cd
;
quit;
*/
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
 	group = "0_HISTORY";
run;

proc fedsql sessref=casauto;
	create table casuser.data_fcst{options replace=true} as
	select product_id, sales_dt
		, sum(P_SUM_QTY) as sum_qty
	from MN_SHORT.PMIX_DAYS_RESULT
	where channel_cd = 1
	group by product_id, sales_dt
	;
quit;

data casuser.data_fcst;
	set casuser.data_fcst;
 	group = "1_ML_FCST";
run;

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

proc casutil;
	droptable 
		casdata="data_union_pmix_by_sku" 
		incaslib="max_casl" 
		quiet
	;
run;

data max_casl.data_union_pmix_by_sku;
	set 
		casuser.data_hist
		casuser.data_fcst
		casuser.data_wplm
		casuser.data_brec
		casuser.data_arec
		casuser.data_plan
	;
run;


proc casutil;
	promote 
		casdata="data_union_pmix_by_sku" 
		casout="data_union_pmix_by_sku"  
		incaslib="max_casl" 
		outcaslib="max_casl"
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







