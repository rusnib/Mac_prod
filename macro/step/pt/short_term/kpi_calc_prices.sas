cas casauto;
caslib _all_ assign;

options casdatalimit=600000M;

%let START_DT   =  '01dec2020'd;
%let END_DT     =  '31mar2021'd;
%let default_vat = 0.2;
%let out_table	= KPI_PRICES;

%let lmvStartDateFormatted = %str(date%')%sysfunc(putn(&START_DT., yymmdd10.))%str(%');
%let lmvEndDateFormatted   = %str(date%')%sysfunc(putn(&END_DT.  , yymmdd10.))%str(%');

/* НА БАЗЕ ФАКТИЧЕСКИХ ЦЕН */

/* Фактическая средняя цена из истории продаж на день */
proc fedsql sessref=casauto;
	create table CASUSER.OPTION_ACT {options replace=true} as
	select
		  PBO_LOCATION_ID
		, product_id
		, SALES_DT
		, intnx('month', SALES_DT, 0, 'B') as month_dt
		, net_sales_amt as SALE_ACT
		, sum(coalesce(sales_qty, 0), coalesce(sales_qty_promo,0)) as UNITS_ACT
		, divide(
			  net_sales_amt
			, sum(coalesce(sales_qty, 0), coalesce(sales_qty_promo,0))
		  ) as avg_act_price_net
	from MN_SHORT.PMIX_SALES 
	where sales_dt <= intnx('month', &lmvEndDateFormatted.  , +1, 'E')
	  and sales_dt >= intnx('month', &lmvStartDateFormatted., -1, 'B')  
	  and channel_cd = 'ALL'
	  and sum(coalesce(sales_qty, 0), coalesce(sales_qty_promo,0)) > 0
	;
quit;

proc fedsql sessref=casauto;
	create table CASUSER.OPTION_PBO_SKU_DAY_V1 {options replace=true} as
	select
		  pbo_location_id
		, product_id
		, SALES_DT
		, avg(avg_act_price_net) as avg_act_price_net
	from CASUSER.OPTION_ACT
	group by
		  pbo_location_id
		, product_id
		, SALES_DT
	;

	create table CASUSER.OPTION_PBO_SKU_MONTH_V2 {options replace=true} as
	select
		  pbo_location_id
		, product_id
		, month_dt
		, avg(avg_act_price_net) as avg_act_price_net
	from CASUSER.OPTION_ACT
	group by
		  pbo_location_id
		, product_id
		, month_dt
	;

	create table CASUSER.OPTION_SKU_DAY_V3 {options replace=true} as
	select
		  product_id
		, sales_dt
		, avg(avg_act_price_net) as avg_act_price_net
	from CASUSER.OPTION_ACT
	group by
		  product_id
		, sales_dt
	;
	
	create table CASUSER.OPTION_SKU_MONTH_V4 {options replace=true} as
	select
		  product_id
		, month_dt
		, avg(avg_act_price_net) as avg_act_price_net
	from CASUSER.OPTION_ACT
	group by
		  product_id
		, month_dt
	;
	
	create table CASUSER.OPTION_PBO_SKU_PERIOD_V5 {options replace=true} as
	select
		  pbo_location_id
		, product_id		
		, avg(avg_act_price_net) as avg_act_price_net
	from CASUSER.OPTION_ACT
	group by
		  pbo_location_id
		, product_id
	;
	
	create table CASUSER.OPTION_SKU_PERIOD_V6 {options replace=true} as
	select
		  product_id		
		, avg(avg_act_price_net) as avg_act_price_net
	from CASUSER.OPTION_ACT
	group by
		  product_id
	;
quit;

/* НА БАЗЕ РАССЧЕТНЫХ ПО АЛГОРИТМУ ЦЕН */
/* Основная таблица */
proc casutil;
    droptable casdata="price_full_sku_pbo_day" incaslib="mn_dict" quiet;
    load casdata="price_full_sku_pbo_day.sashdat" incaslib="mn_dict" casout="price_full_sku_pbo_day" outcaslib="mn_dict";
quit;
data casuser.prices;
	set MN_DICT.PRICE_FULL_SKU_PBO_DAY;
	where period_dt between &START_DT. and &END_DT.
/* 		and channel_cd='ALL'  */
	;
run;
proc casutil;
    droptable casdata="price_full_sku_pbo_day" incaslib="mn_dict" quiet;
quit;



/* СТРУКТУРА */

data CASUSER.CALENDAR;
	do i = &START_DT. to &END_DT.;
		output;
	end;
run;

proc fedsql sessref=casauto;
	create table CASUSER.STRUCTURE {options replace=true} as
	select 
		  pbo.pbo_location_id
		, sku.product_id
		, cast(cal.i as date) as period_dt
		, cast(intnx('month', cal.i, 0, 'B') as date) as month_dt

	from 
		(select * from CASUSER.CALENDAR 
			where i not between %str(date%')%sysfunc(putn('1feb2021'd, yymmdd10.))%str(%')
			and  %str(date%')%sysfunc(putn('28feb2021'd, yymmdd10.))%str(%')
		) as cal
	cross join 
		(select * from MN_SHORT.PBO_DICTIONARY 
			where A_CLOSE_DATE > &lmvStartDateFormatted.
				or A_OPEN_DATE < &lmvEndDateFormatted.
		) as pbo
	cross join
		MN_SHORT.PRODUCT_DICTIONARY as sku
	;
quit;


/* Финальные цены */
proc casutil;
	droptable 
		casdata		= "&out_table." 
		incaslib	= "MAX_CASL" 
		quiet         
	;                 
run;    

proc fedsql sessref=casauto;
		create table MAX_CASL.&out_table.{options replace=true} as
	select 
		  main.product_id
		, main.pbo_location_id
		, main.period_dt
		, coalesce(
			  v0.price_net
			, v0.price_gross * (1 - &default_vat.)
			, v1.avg_act_price_net
			, v2.avg_act_price_net
			, v3.avg_act_price_net
			, v4.avg_act_price_net
			, v5.avg_act_price_net
			, v6.avg_act_price_net
		  ) as price_net
		, v0.price_net as price_net_curr 

	from CASUSER.STRUCTURE as main
		  
	left join CASUSER.prices 	as v0	
		on  main.product_id			= v0.product_id 
		and main.pbo_location_id	= v0.pbo_location_id 
		and main.period_dt			= v0.period_dt


	left join CASUSER.OPTION_PBO_SKU_DAY_V1 	as v1	
		on  main.product_id			= v1.product_id 
		and main.pbo_location_id	= v1.pbo_location_id 
		and main.period_dt			= v1.sales_dt

	left join CASUSER.OPTION_PBO_SKU_MONTH_V2 	as v2
		on  main.product_id			= v2.product_id 
		and main.pbo_location_id	= v2.pbo_location_id 
		and main.month_dt			= v2.month_dt
		
	left join CASUSER.OPTION_SKU_DAY_V3 		as v3
		on  main.product_id			= v3.product_id 
		and main.period_dt			= v3.sales_dt

	left join CASUSER.OPTION_SKU_MONTH_V4 		as v4
		on  main.product_id			= v4.product_id 
		and main.month_dt			= v4.month_dt
		
	left join CASUSER.OPTION_PBO_SKU_PERIOD_V5 	as v5
		on  main.product_id			= v5.product_id 
		and main.pbo_location_id	= v5.pbo_location_id 
		
	left join CASUSER.OPTION_SKU_PERIOD_V6 		as v6
		on  main.product_id			= v6.product_id 
	;
quit;

proc casutil;         
	promote           
		casdata		= "&out_table." 
		incaslib	= "MAX_CASL" 
		casout		= "&out_table."  
		outcaslib	= "MAX_CASL"
	;                 
run;    

/* 117.044.359 / 225.035.355 = 52% */
data casuser.test1;
set MAX_CASL.&out_table.;
where price_net is missing;
run;

/* 142.174.439 / 225.035.355 = 63% */
data casuser.test2;
set MAX_CASL.&out_table.;
where price_net_curr is missing;
run;