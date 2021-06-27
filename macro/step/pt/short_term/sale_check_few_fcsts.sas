cas casauto;
caslib _all_ assign;
    
/* PMIX_DAYS_RESULT (scoring) -> plan_*_month\day */

/* Сумма до pbo по всем ПБО */
proc fedsql sessref=casauto;
	create table casuser.data_hist{options replace=true} as
	select pbo_location_id, sales_dt
/* 		, sum(coalesce(sales_qty, 0) + coalesce(sales_qty_promo,0)) as sum_qty */
		, sum(net_sales_amt) as sum_rur
	from MN_SHORT.PMIX_SALES
	where channel_cd = 'ALL'
	group by pbo_location_id, sales_dt
;
quit;

data casuser.data_hist;
	set casuser.data_hist;
 	group = "HIST";
run;

/* before comp */
proc fedsql sessref=casauto;
	create table casuser.data_bcmp{options replace=true} as
	select pbo_location_id, sales_dt
		, sum(FCST_SALE) as sum_rur
	from MAX_CASL.SHARE_FCST_UNITS_N_SALE
	group by pbo_location_id, sales_dt
;
quit;

data casuser.data_bcmp;
	set casuser.data_bcmp;
 	group = "BCMP";
run;

/* after comp */
proc fedsql sessref=casauto;
	create table casuser.data_acmp{options replace=true} as
	select pbo_location_id, sales_dt
		, sum(FCST_SALE) as sum_rur
	from casuser.sale_only_comp
	group by pbo_location_id, sales_dt
;
quit;


data casuser.data_acmp;
	set casuser.data_acmp;
 	group = "ACMP";
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
		casuser.data_bcmp
		casuser.data_acmp
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