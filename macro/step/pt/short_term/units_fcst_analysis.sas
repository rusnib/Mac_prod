
proc fedsql sessref=casauto;
	create table casuser.test_hist{options replace=true} as
	select channel_cd
		, count(sales_dt) as obs
		, count(distinct sales_dt) as count_dts
		, count(distinct PBO_LOCATION_ID) as count_loc
		, count(distinct PRODUCT_ID) as count_sku
		, sum(sales_qty) as sales_qty
		, sum(sales_qty_promo) as sales_qty_promo 
	from MN_SHORT.PMIX_SALES
	group by channel_cd
;
quit;

proc fedsql sessref=casauto;
	create table casuser.test_fcst{options replace=true} as
	select channel_cd
		, count(sales_dt) as obs
		, count(distinct sales_dt) as count_dts
		, count(distinct PBO_LOCATION_ID) as count_loc
		, count(distinct PRODUCT_ID) as count_sku
		, sum(p_sum_qty) as p_sum_qty
	from MN_SHORT.PMIX_DAYS_RESULT
	group by channel_cd
;
quit;


proc fedsql sessref=casauto;
	create table casuser.test_wplm{options replace=true} as
	select channel_cd
		, count(period_dt) as obs
		, count(distinct period_dt) as count_dts
		, count(distinct PBO_LOCATION_ID) as count_loc
		, count(distinct PRODUCT_ID) as count_sku
		, sum(FF) as FF
	from MN_SHORT.FC_W_PLM
	where period_dt <= date '2021-08-22'
	group by channel_cd
;
quit;

proc fedsql sessref=casauto;
	create table casuser.test_plan{options replace=true} as
	select count(data) as obs
		, count(distinct data) as count_dts
		, count(distinct LOCATION) as count_loc
		, count(distinct PROD) as count_sku
		, sum(FINAL_FCST_UNITS) as final_qty
		, sum(OVERRIDED_FCST_UNITS) as overd_fcst 
		, sum(PROMO_FCST_UNITS) as promo_fcst 
	from MN_SHORT.PLAN_PMIX_DAY
;
quit;

proc fedsql sessref=casauto;
	create table casuser.test_frec{options replace=true} as
	select channel_cd
		, count(period_dt) as obs
		, count(distinct period_dt) as count_dts
		, count(distinct PBO_LOCATION_ID) as count_loc
		, count(distinct PRODUCT_ID) as count_sku
		, sum(FF) as FF
	from MN_SHORT.fcst_reconciled
	where period_dt <= date '2021-08-22'
	group by channel_cd
;
quit;




proc fedsql sessref=casauto;
	create table casuser.test_ffvf{options replace=true} as
	select count(sales_dt) as obs
		, count(distinct sales_dt) as count_dts
		, count(distinct PBO_LOCATION_ID) as count_loc
		, sum(pbo_fcst) as FF
	from MN_DICT.PBO_FORECAST_RESTORED
;
quit;
