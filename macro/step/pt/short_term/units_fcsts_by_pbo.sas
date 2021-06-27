
cas casauto;
caslib _all_ assign;
    
/* PMIX_DAYS_RESULT (scoring) -> plan_*_month\day */

/* Сумма до pbo по всем ПБО */
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
 	group = "HIST";
run;

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
 	group = "FCST";
run;

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
 	group = "PLAN";
run;

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
 	group = "WPLM";
run;

proc fedsql sessref=casauto;
	create table casuser.data_wout{options replace=true} as
	select pbo_location_id, period_dt as sales_dt
		, sum(FF) as sum_qty
	from MN_SHORT.FC_WO_PLM
	where channel_cd = 'ALL'
	group by pbo_location_id, period_dt
;
quit;

data casuser.data_wout;
	set casuser.data_wout;
 	group = "WOUT";
run;

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
 	group = "FREC";
run;

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
		casuser.data_wout
		casuser.data_frec
	;
run;


proc casutil;
	promote 
		casdata="data_union_pmix_by_pbo" 
		casout="data_union_pmix_by_pbo"  
		incaslib="casuser" 
		outcaslib="casuser"
	;
run;

/*
сравнить wplm и data (должны совпадать)
посмотреть дубли

реконсиляция после новинок и plm!





