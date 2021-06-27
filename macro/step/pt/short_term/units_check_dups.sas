cas casauto;
caslib _all_ assign;

/* 106.588.027 */
/*  97.449.989 */

/* DISTINCT */
/* proc fedsql sessref=casauto; */
/* 	create table casuser.distinct_score{options replace=true} as */
/* 	select distinct sales_dt, product_id, pbo_location_id, CHANNEL_CD */
/* 	from MN_SHORT.ALL_ML_SCORING */
/* 	; */
/* quit; */

proc fedsql sessref=casauto;
	create table casuser.distinct_planday{options replace=true} as
	select distinct DATA, PROD, LOCATION
	from MN_SHORT.PLAN_PMIX_DAY
	;
quit;

proc fedsql sessref=casauto;
	create table casuser.distinct_dayres{options replace=true} as
	select distinct sales_dt, product_id, pbo_location_id, CHANNEL_CD
	from MN_SHORT.PMIX_DAYS_RESULT
	;
quit;

proc fedsql sessref=casauto;
	create table casuser.distinct_npf{options replace=true} as
	select distinct sales_dt, product_id, pbo_location_id, CHANNEL_CD
	from MN_SHORT.NPF_PREDICTION
	;
quit;

proc fedsql sessref=casauto;
	create table casuser.distinct_pmxdaily{options replace=true} as
	select distinct period_dt, product_id, pbo_location_id, CHANNEL_CD
	from MN_SHORT.PMIX_DAILY
	;
quit;

proc fedsql sessref=casauto;
	create table casuser.distinct_fcwplm{options replace=true} as
	select distinct period_dt, product_id, pbo_location_id, CHANNEL_CD
	from MN_SHORT.FC_W_PLM
	;
quit;

proc fedsql sessref=casauto;
	create table casuser.distinct_nnet{options replace=true} as
	select distinct week_dt, product_id, pbo_location_id, CHANNEL_CD
	from CASUSER.NNET_WP_SCORED1
	;
quit;

proc fedsql sessref=casauto;
	create table casuser.distinct_caspmixdaily{options replace=true} as
	select distinct period_dt, product_id, pbo_location_id, CHANNEL_CD
	from CASUSER.PMIX_DAILY
	;
quit;

proc fedsql sessref=casauto;
	create table casuser.step_1{options replace=true} as
	select  period_dt, product_id, pbo_location_id, CHANNEL_CD, count(period_dt) as count
	from MN_SHORT.PMIX_DAILY
	group by  period_dt, product_id, pbo_location_id, CHANNEL_CD
	having count(period_dt) > 1
	;
create table casuser.step_2{options replace=true} as
	select distinct period_dt
	from CASUSER.step_1
	;
quit;




proc fedsql sessref=casauto;
	create table casuser.distinct_am1{options replace=true} as
	select distinct product_id, pbo_location_id, start_dt, end_dt
	from MN_SHORT.ASSORT_MATRIX
	;
create table casuser.distinct_am2{options replace=true} as
	select distinct product_id, pbo_location_id
	from MN_SHORT.ASSORT_MATRIX
	;
quit;


/* COUNT BY CHANNEL */
proc fedsql sessref=casauto;
	create table casuser.count_score{options replace=true} as
	select CHANNEL_CD
		, count(sales_dt) as count_obs
		, count(distinct sales_dt) as count_dts
		, count(distinct product_id) as count_sku
		, count(distinct pbo_location_id) as count_loc
	from MN_SHORT.ALL_ML_SCORING
	group by CHANNEL_CD
	;
quit;


proc fedsql sessref=casauto;
	create table casuser.count_score_distinct{options replace=true} as
	select CHANNEL_CD
		, count(sales_dt) as count_obs
		, count(distinct sales_dt) as count_dts
		, count(distinct product_id) as count_sku
		, count(distinct pbo_location_id) as count_loc
	from casuser.distinct_score
	group by CHANNEL_CD
	;
quit;




proc fedsql sessref=casauto;
	create table casuser.test_filt_dups{options replace=true} as
	select CHANNEL_CD, product_id, pbo_location_id, sales_dt
		, count(sales_dt) as count_dts
	from MN_SHORT.ALL_ML_SCORING
	where CHANNEL_CD = 1
	group by CHANNEL_CD, product_id, pbo_location_id, sales_dt
	having count(sales_dt) > 1
	;
quit;



proc fedsql sessref=casauto;
	create table casuser.filt_dups_dts{options replace=true} as
	select sales_dt
		, count(product_id) as count_rows
	from casuser.filt_dups
	group by sales_dt
	;
quit;

proc fedsql sessref=casauto;
	create table casuser.filt_dups_pairs{options replace=true} as
	select product_id, pbo_location_id 
		, count(sales_dt) as count_rows
	from casuser.filt_dups
	group by  product_id, pbo_location_id 
	;
quit;