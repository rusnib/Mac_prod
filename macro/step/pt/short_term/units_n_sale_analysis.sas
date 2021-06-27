cas casauto;
caslib _all_ assign;
    
/* PMIX_DAYS_RESULT (scoring) -> plan_*_month\day */

/* Сумма до pbo по всем ПБО */
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
 	group = "HIST";
run;

/* proc fedsql sessref=casauto; */
/* 	create table casuser.data_fcst{options replace=true} as */
/* 	select product_id, sales_dt */
/* 		, sum(FINAL_FCST_UNITS) as sum_qty */
/* 		, sum(FCST_SALE) as sum_rur */
/* 	from    MAX_CASL.FINAL_FCST_1 */
/* 	group by product_id, sales_dt */
/* ; */
/* quit; */
/*  */
/* data casuser.data_fcst; */
/* 	set casuser.data_fcst; */
/*  	group = "FCST"; */
/* run; */

/* proc fedsql sessref=casauto; */
/* 	create table casuser.data_fcst2{options replace=true} as */
/* 	select product_id, sales_dt */
/* 		, sum(FCST_UNITS) as sum_qty */
/* 		, sum(FCST_SALE) as sum_rur */
/* 	from  MAX_CASL.FINAL_FCST_07062021 */
/* 	group by product_id, sales_dt */
/* ; */
/* quit; */
/*  */
/* data casuser.data_fcst2; */
/* 	set casuser.data_fcst2; */
/*  	group = "FC_2"; */
/* run; */
/*  */


proc fedsql sessref=casauto;
	create table casuser.data_plan{options replace=true} as
	select 
		  PROD as product_id, data as sales_dt
		, sum(FINAL_FCST_SALE) as sum_rur
/* 		, sum(OVERRIDED_FCST_UNITS) as overd_fcst  */
/* 		, sum(PROMO_FCST_UNITS) as promo_fcst  */
	from MN_SHORT.PLAN_PMIX_DAY
	group by PROD, data
;
quit;

data casuser.data_plan;
	set casuser.data_plan;
 	group = "PLAN";
run;



proc casutil;
	droptable 
		casdata="data_union_sale_by_sku" 
		incaslib="casuser" 
		quiet
	;
run;


data casuser.data_union_sale_by_sku;
	set 
		casuser.data_hist
/* 		casuser.data_fcst */
		casuser.data_plan
	;
run;


proc casutil;
	promote 
		casdata="data_union_sale_by_sku" 
		casout="data_union_sale_by_sku"  
		incaslib="casuser" 
		outcaslib="casuser"
	;
run;

/*
сравнить wplm и data (должны совпадать)
посмотреть дубли

реконсиляция после новинок и plm!


