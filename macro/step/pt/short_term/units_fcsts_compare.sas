/* Sum by PBO */

cas casauto;
caslib _all_ assign;
    

/* 0 */
/* History */
proc fedsql sessref=casauto;
	create table casuser.data_hist{options replace=true} as
	select pbo_location_id, sales_dt
		, sum(coalesce(sales_qty, 0) + coalesce(sales_qty_promo,0)) as sum_qty
	from MN_SHORT.PMIX_SALES
	where channel_cd = 'ALL'
	group by pbo_location_id, sales_dt
;
quit;

data casuser.data_hist;
	set casuser.data_hist;
 	group = "0_HIST";
run;

/* 1 */
/* FCST = ML forecast */
/* PMIX_DAYS_RESULT (scoring) -> plan_*_month\day */
proc fedsql sessref=casauto;
	create table casuser.data_fcst{options replace=true} as
	select pbo_location_id, sales_dt
		, sum(P_SUM_QTY) as sum_qty
	from MN_SHORT.PMIX_DAYS_RESULT
	where channel_cd = 2
	group by pbo_location_id, sales_dt
;
quit;

data casuser.data_fcst;
	set casuser.data_fcst;
 	group = "1_FCST";
run;

/* 2 */
/* WPLM = ML forecast after PLM and new products */
proc fedsql sessref=casauto;
	create table casuser.data_wplm{options replace=true} as
	select pbo_location_id, period_dt as sales_dt
		, sum(FF) as sum_qty
	from MN_SHORT.FC_W_PLM
	where channel_cd = 'ALL'
	group by pbo_location_id, period_dt
;
quit;

data casuser.data_wplm;
	set casuser.data_wplm;
 	group = "2_WPLM";
run;

/* 3 */
/* FFVF = PBO_FORECAST_RESTORED from VF */
proc fedsql sessref=casauto;
	create table casuser.data_ffvf{options replace=true} as
	select pbo_location_id,  sales_dt
		, sum(PBO_FCST) as sum_qty
	from MN_DICT.PBO_FORECAST_RESTORED
	where channel_cd = 'ALL'
	group by pbo_location_id, sales_dt
	;
quit;

data casuser.data_ffvf;
	set casuser.data_ffvf;
 	group = "3_FFVF";
run;

/* 4 */
/* FREC = reconciled WPLM forecast */
proc fedsql sessref=casauto;
	create table casuser.data_frec{options replace=true} as
	select pbo_location_id, period_dt as sales_dt
		, sum(FF) as sum_qty
	from MN_SHORT.fcst_reconciled
	where channel_cd = 'ALL'
	group by pbo_location_id, period_dt
	;
quit;

data casuser.data_frec;
	set casuser.data_frec;
 	group = "4_FREC";
run;

/* 5 */
/* PLAN = final forecast to DP */
proc fedsql sessref=casauto;
	create table casuser.data_plan{options replace=true} as
	select 
		  LOCATION as pbo_location_id, data as sales_dt
		, sum(FINAL_FCST_UNITS) as sum_qty
/* 		, sum(OVERRIDED_FCST_UNITS) as overd_fcst  */
/* 		, sum(PROMO_FCST_UNITS) as promo_fcst  */
	from MN_SHORT.PLAN_PMIX_DAY
	group by LOCATION, data
;
quit;

data casuser.data_plan;
	set casuser.data_plan;
 	group = "5_PLAN";
run;

/* UNION to ONE table */
proc casutil;
	droptable 
		casdata="data_union_pmix_by_pbo" 
		incaslib="casuser" 
		quiet
	;
run;

data casuser.data_union_pmix_by_pbo;
	set 
		casuser.data_hist
		casuser.data_fcst
		casuser.data_wplm
		casuser.data_plan
/* 		casuser.data_wout */
		casuser.data_frec
		casuser.data_ffvf
	;
	where sales_dt <= '1sep2021'd;
run;


proc casutil;
	promote 
		casdata="data_union_pmix_by_pbo" 
		casout="data_union_pmix_by_pbo"  
		incaslib="casuser" 
		outcaslib="casuser"
	;
run;






/* Sum by PBO */

cas casauto;
caslib _all_ assign;
    

/* 0 */
/* History */
proc fedsql sessref=casauto;
	create table casuser.data_hist{options replace=true} as
	select pbo_location_id, sales_dt
		, sum(coalesce(sales_qty, 0) + coalesce(sales_qty_promo,0)) as sum_qty
	from MN_SHORT.PMIX_SALES
	where channel_cd = 'ALL'
	group by pbo_location_id, sales_dt
;
quit;

data casuser.data_hist;
	set casuser.data_hist;
 	group = "0_HIST";
run;

/* 1 */
/* FCST = ML forecast */
/* PMIX_DAYS_RESULT (scoring) -> plan_*_month\day */
proc fedsql sessref=casauto;
	create table casuser.data_fcst{options replace=true} as
	select pbo_location_id, sales_dt
		, sum(P_SUM_QTY) as sum_qty
	from MN_SHORT.PMIX_DAYS_RESULT
	where channel_cd = 2
	group by pbo_location_id, sales_dt
;
quit;

data casuser.data_fcst;
	set casuser.data_fcst;
 	group = "1_FCST";
run;

/* 2 */
/* WPLM = ML forecast after PLM and new products */
proc fedsql sessref=casauto;
	create table casuser.data_wplm{options replace=true} as
	select pbo_location_id, product_id,period_dt
		, avg(FF) as FF
	from MN_SHORT.FC_W_PLM
	where channel_cd = 'ALL' and period_dt <= date '2021-08-22'
	group by pbo_location_id, product_id,period_dt
;
create table casuser.data_wplm{options replace=true} as
	select pbo_location_id, period_dt as sales_dt
		, sum(FF) as sum_qty
	from casuser.data_wplm
	group by pbo_location_id, period_dt;
 
quit;

data casuser.data_wplm;
	set casuser.data_wplm;
 	group = "2_WPLM";
run;

/* 3 */
/* FFVF = PBO_FORECAST_RESTORED from VF */
proc fedsql sessref=casauto;
	create table casuser.data_ffvf{options replace=true} as
	select pbo_location_id,  sales_dt
		, sum(PBO_FCST) as sum_qty
	from MN_DICT.PBO_FORECAST_RESTORED
	where channel_cd = 'ALL' and sales_dt <= date '2021-08-22'
	group by pbo_location_id, sales_dt
	;
quit;

data casuser.data_ffvf;
	set casuser.data_ffvf;
 	group = "3_FFVF";
run;

/* 4 */
/* FREC = reconciled WPLM forecast */
proc fedsql sessref=casauto;
	create table casuser.data_frec{options replace=true} as
	select pbo_location_id, product_id, period_dt
		, avg(FF) as FF
	from MN_SHORT.fcst_reconciled
	where channel_cd = 'ALL' and period_dt <= date '2021-08-22'
	group by pbo_location_id,product_id, period_dt
	;
create table casuser.data_frec{options replace=true} as
	select pbo_location_id, period_dt as sales_dt
		, sum(FF) as sum_qty
	from casuser.data_frec
	group by pbo_location_id, period_dt
	;
quit;

data casuser.data_frec;
	set casuser.data_frec;
 	group = "4_FREC";
run;

/* 5 */
/* PLAN = final forecast to DP */
proc fedsql sessref=casauto;
	create table casuser.data_plan{options replace=true} as
	select 
		  LOCATION, PROD, data
		, avg(FINAL_FCST_UNITS) as FINAL_FCST_UNITS
/* 		, sum(OVERRIDED_FCST_UNITS) as overd_fcst  */
/* 		, sum(PROMO_FCST_UNITS) as promo_fcst  */
	from MN_SHORT.PLAN_PMIX_DAY
	where  data <= date '2021-08-22'
	group by LOCATION, PROD, data
	;
	create table casuser.data_plan{options replace=true} as
	select 
		  LOCATION as pbo_location_id, data as sales_dt
		, sum(FINAL_FCST_UNITS) as sum_qty
/* 		, sum(OVERRIDED_FCST_UNITS) as overd_fcst  */
/* 		, sum(PROMO_FCST_UNITS) as promo_fcst  */
	from casuser.data_plan
	group by LOCATION, data
	;
quit;

data casuser.data_plan;
	set casuser.data_plan;
 	group = "5_PLAN";
run;

/* 6 */
/* FFVF = PBO_FORECAST_RESTORED from VF */
proc fedsql sessref=casauto;
	create table casuser.data_manl{options replace=true} as
	select pbo_location_id,  sales_dt
		, sum(FINAL_FCST_UNITS) as sum_qty
	from MAX_CASL.FINAL_FCST
/* 	where channel_cd = 'ALL' and sales_dt <= date '2021-08-22' */
	group by pbo_location_id, sales_dt
	;
quit;

data casuser.data_manl;
	set casuser.data_manl;
 	group = "6_MANL";
run;




/* UNION to ONE table */
proc casutil;
	droptable 
		casdata="data_union_pmix_by_pbo" 
		incaslib="casuser" 
		quiet
	;
run;

data casuser.data_union_pmix_by_pbo;
	set 
		casuser.data_hist
		casuser.data_fcst
		casuser.data_wplm
		casuser.data_plan
		casuser.data_manl
		casuser.data_frec
		casuser.data_ffvf
	;
	where sales_dt <= '22aug2021'd;
run;


proc casutil;
	promote 
		casdata="data_union_pmix_by_pbo" 
		casout="data_union_pmix_by_pbo"  
		incaslib="casuser" 
		outcaslib="casuser"
	;
run;




