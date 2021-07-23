%macro assign;
%let casauto_ok = %sysfunc(SESSFOUND ( cmasauto)) ;
%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
 cas casauto;
 caslib _all_ assign;
%end;
%mend;
%assign

options casdatalimit=600000M;

%add_promotool_marks2(mpOutCaslib=casuser,
							mpPtCaslib=pt,
							PromoCalculationRk=);

/* Поднятие в CAS истории чеков */
%let lmvReportDttm 	       = &ETL_CURRENT_DTTM.;
/* data CASUSER.PBO_SALES (replace=yes drop=valid_from_dttm valid_to_dttm); */
/* 	set ETL_IA.PBO_SALES (where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.)); */
/* run; */

/* data CASUSER.PMIX_SALES (replace=yes drop=valid_from_dttm valid_to_dttm); */
/* 	set ETL_IA.PMIX_SALES (where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.)); */
/* run; */



/* Изменяемые параметры */
/*
01MAR2021		31MAR2021
01DEC2020		31DEC2020
01JAN2021		31JAN2021
*/

%let lmvStartDate 	= '01MAR2021'd;
%let lmvEndDate 	= '31MAR2021'd;

%let lmvTableFcst 	= MAX_CASL.FCST_UNITS_MAR;
%let lmvTableFcstGc = MAX_CASL.GC_FORECAST_RESTORED_MAR;
%let lmvOutTablePostfix = MAR;


/* Неизменяемые параметры */

%let lmvExcludingList 	   = 9908, 1494, 1495, 1496, 1497, 1498, 1499 ;
%let lmvStartDateFormatted = %str(date%')%sysfunc(putn(&lmvStartDate., yymmdd10.))%str(%');
%let lmvEndDateFormatted   = %str(date%')%sysfunc(putn(&lmvEndDate.  , yymmdd10.))%str(%');
%let lmvTestMonthDate 	   = %str(date%')%sysfunc(putn(%sysfunc(intnx(month,&lmvStartDate.,0)), yymmdd10.))%str(%');

%let lmvPriceFcst = MAX_CASL.KPI_PRICES;






/************************************************************************************/
/******************************* 2.1 Restaurants list *******************************/
/************************************************************************************/

/* Step 0. Closed dates */

/* ------------ Start. Дни когда пбо будет уже закрыт (навсегда) ------------------ */
	data casuser.days_pbo_date_close;
		set MN_SHORT.PBO_DICTIONARY;
		format period_dt date9.;
		keep PBO_LOCATION_ID CHANNEL_CD period_dt;
		CHANNEL_CD = "ALL"; 
		if A_CLOSE_DATE ne . and A_CLOSE_DATE <= &lmvEndDate. then 
		do period_dt = max(A_CLOSE_DATE, &lmvStartDate.) to &lmvEndDate.;
			output;
		end;
	run;
/* ------------ End. Дни когда пбо будет уже закрыт (навсегда) -------------------- */


/* ------------ Start. Дни когда пбо будет временно закрыт ------------------------ */
	data casuser.days_pbo_close;
		set MN_SHORT.PBO_CLOSE_PERIOD;
		format period_dt date9.;
		keep PBO_LOCATION_ID CHANNEL_CD period_dt;
		if channel_cd = "ALL" ;
		if (lmvEndDate >= &lmvStartDate. and lmvEndDate <= &lmvEndDate.) 
		or (lmvStartDate >= &lmvStartDate. and lmvStartDate <= &lmvEndDate.) 
		or (lmvStartDate <= &lmvStartDate. and &lmvStartDate. <= lmvEndDate)
		then
		do period_dt = max(lmvStartDate, &lmvStartDate.) to min(&lmvEndDate., lmvEndDate);
			output;
		end;
	run;
/* ------------ End. Дни когда пбо будет временно закрыт -------------------------- */


/* ------------ Start. Дни когда закрыто ПБО - никаких продаж быть не должно ------ */
	data casuser.days_pbo_close(append=force); 
	  set casuser.days_pbo_date_close;
	run;
/* ------------ End. Дни когда закрыто ПБО - никаких продаж быть не должно -------- */

	
/* ------------ Start. Убираем дубликаты ------------------------------------------ */
	proc fedsql sessref = casauto;
	create table casuser.days_pbo_close{options replace=true} as
	select distinct * from casuser.days_pbo_close;
	quit;
/* ------------ End. Убираем дубликаты -------------------------------------------- */

/* ------------ Start. Сколько дней в месяце ресторан закрыт ?  ------------------- */
	proc fedsql sessref = casauto;
		create table casuser.num_days_pbo_close{options replace=true} as
		select 
			  pbo_location_id
			, cast(intnx('month', period_dt, 0, 'B') as date) as month_dt
			, count(period_dt) as num_days_pbo_close
		from casuser.days_pbo_close
		group by 1,2
		;
	quit;
/* ------------ End. Сколько дней в месяце ресторан закрыт ?  --------------------- */


/* Step 1. Comparable PBOs */

/* Список всех ПБО из справочника и дат их открытия-закрытия*/
/* %let common_path = /opt/sas/mcd_config/macro/step/pt/alerts; */
/* %include "&common_path./data_prep_pbo.sas";  */
/* %data_prep_pbo( */
/* 	  mpInLib 		= ETL_IA */
/* 	, mpReportDttm 	= &ETL_CURRENT_DTTM. */
/* 	, mpOutCasTable = CASUSER.PBO_DICTIONARY */
/* ); */


/* Расчет комповых ресторанов */
proc fedsql sessref=casauto;
	create table CASUSER.PBO_LIST_COMP {options replace=true} as
	select
		  pbo_location_id
		, A_OPEN_DATE
		, A_CLOSE_DATE
	from 
/* 		CASUSER.PBO_DICTIONARY */
	MN_SHORT.PBO_DICTIONARY

	where 
		intnx('month', &lmvTestMonthDate. , -12, 'b') >= 
      		case 
	   			when day(A_OPEN_DATE)=1 
					then cast(A_OPEN_DATE as date)
	   			else 
					cast(intnx('month', A_OPEN_DATE, 1, 'b') as date)
      		end
	    and &lmvTestMonthDate. <=
			case
				when A_CLOSE_DATE is null 
					then cast(intnx('month',  &lmvTestMonthDate., 12) as date)
				when A_CLOSE_DATE=intnx('month', A_CLOSE_DATE, 0, 'e') 
					then cast(A_CLOSE_DATE as date)
		   		else 
					cast(intnx('month', A_CLOSE_DATE, -1, 'e') as date)
			end
	;
	/* Кол-во компов: 
		- На марте 2021: 	723
		- На январе 2021: 	723
		- На декабре 2020: 	698
	*/

quit;

/* Step 2. All days higher than 100 gc */
proc fedsql sessref=casauto;
	create table CASUSER.PBO_GC_OVER100 {options replace=true} as
	select PBO_LOCATION_ID
		, count(SALES_DT) as count_days 								/* Кол-во дней после фильтрации */
	from CASUSER.PBO_SALES 
	where RECEIPT_QTY > 100												/* Фильтр на 100 чеков */
		and SALES_DT >= &lmvStartDateFormatted. 						/* Фильтр на тестовый период */
		and SALES_DT <= &lmvEndDateFormatted.							
		and CHANNEL_CD = 'ALL'											/* Фильтр на канал !!! */
	group by PBO_LOCATION_ID
	;
quit;

proc fedsql sessref=casauto;
	create table CASUSER.PBO_LIST_GC_OVER100 {options replace=true} as
	select main.PBO_LOCATION_ID
		, main.count_days 	
		, cl.num_days_pbo_close
	from CASUSER.PBO_GC_OVER100 as main
	left join casuser.num_days_pbo_close as cl
		on main.PBO_LOCATION_ID = cl.PBO_LOCATION_ID
	/* Проверяем, что после фильтрации кол-во осташихся дней продаж равно кол-во дней в тестовом месяце  
		с учетом выброшенных в закрытиях и временных закрытиях дней*/
	where main.count_days + coalesce(cl.num_days_pbo_close, 0) = 1 + intck('day', &lmvStartDateFormatted., &lmvEndDateFormatted.) 
		and main.count_days > 0
	;
quit;


/* Step 3. Only with sales history */
proc fedsql sessref=casauto;
	create table CASUSER.PBO_LIST_QTY_OVER0 {options replace=true} as
	select distinct PBO_LOCATION_ID
	from MN_SHORT.PMIX_SALES
	where sales_dt <= &lmvEndDateFormatted.  
	  and sales_dt >= &lmvStartDateFormatted.  
	  and channel_cd = 'ALL'
	  and sum(coalesce(sales_qty, 0), coalesce(sales_qty_promo,0)) > 0
	  and product_id not in (&lmvExcludingList.)
	;
quit;


/* final pbo list */
proc fedsql sessref=casauto;
	create table casuser.PBO_LIST{options replace=true} as
	select t1.PBO_LOCATION_ID
	from 
		casuser.PBO_LIST_COMP as t1

	inner join 
		casuser.PBO_LIST_GC_OVER100 as t2
	on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID
	
	inner join 
		casuser.PBO_LIST_QTY_OVER0 as t3
	on t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID
	;
quit;

/* PBO_LIST: остается в пересечении:
	дек 2020		
	янв 2021		640
	мар 2021
*/
	


/************************************************************************************/
/******************************* 2.2 List of products *******************************/
/************************************************************************************/

/* Step 1. Товары из PRODUCT_CHAIN */
proc fedsql sessref=casauto;
	create table casuser.PROD_LIST_PLM{options replace=true} as
	select distinct successor_product_id as product_id
	from casuser.PRODUCT_CHAIN_ENH
	where successor_start_dt <= &lmvEndDateFormatted.  
	  and predecessor_end_dt >= &lmvStartDateFormatted.  
	  and successor_product_id not in (&lmvExcludingList.)
	;
quit;

/* Step 2. Товары из SALES history */
proc fedsql sessref=casauto;
	create table casuser.PROD_LIST_SALES{options replace=true} as
	select distinct product_id
	from MN_SHORT.PMIX_SALES
	where sales_dt <= &lmvEndDateFormatted.  
	  and sales_dt >= &lmvStartDateFormatted.  
	  and channel_cd = 'ALL'
	  and sum(sales_qty, sales_qty_promo) > 0
	  and product_id not in (&lmvExcludingList.)
	;
quit;

/* Final product list */
proc fedsql sessref=casauto;
	create table casuser.PROD_LIST{options replace=true} as
	select t1.product_id
	from casuser.PROD_LIST_PLM as t1
	inner join casuser.PROD_LIST_SALES as t2
		on t1.product_id = t2.product_id
	;
quit;


/**************************************************************************************/
/*********************** ACTUAL & FORECAST DATA PREPARATION ***************************/
/**************************************************************************************/


proc fedsql sessref=casauto;
	/* UNITS_ACT */
	create table CASUSER.UNITS_ACT {options replace=true} as
	select 
		  pbo.product_id
		, cast(intnx('month', pbo.SALES_DT, 0, 'B')	as date) as month_dt
		, sum(coalesce(sales_qty, 0) + coalesce(sales_qty_promo,0)) as UNITS_ACT
	from MN_SHORT.PMIX_SALES as pbo
	inner join CASUSER.PBO_LIST as loc
		on pbo.PBO_LOCATION_ID = loc.PBO_LOCATION_ID
	inner join CASUSER.PROD_LIST as sku
		on pbo.product_id = sku.product_id
	where   pbo.SALES_DT >= &lmvStartDateFormatted. 						
		and pbo.SALES_DT <= &lmvEndDateFormatted.							
		and pbo.CHANNEL_CD = 'ALL'											
	group by 1,2
	;
	/* GC_ACT */
	create table CASUSER.GC_ACT {options replace=true} as
	select sum(pbo.RECEIPT_QTY) as GC_ACT
	from CASUSER.PBO_SALES as pbo
	inner join CASUSER.PBO_LIST as loc
		on pbo.PBO_LOCATION_ID = loc.PBO_LOCATION_ID
	where   pbo.SALES_DT >= &lmvStartDateFormatted. 						
		and pbo.SALES_DT <= &lmvEndDateFormatted.							
		and pbo.CHANNEL_CD = 'ALL'											
	;
	/* UPT_ACT */
	create table CASUSER.UNITS_N_GC_ACT {options replace=true} as
	select units.product_id
		, units.month_dt
		, units.UNITS_ACT
		, gc.GC_ACT
		, divide(1000 * units.UNITS_ACT, gc.GC_ACT) as UPT_ACT
	from CASUSER.UNITS_ACT as units
	cross join CASUSER.GC_ACT as gc
	;
quit;


proc fedsql sessref=casauto;
	/* UNITS_FCST */
	create table CASUSER.UNITS_SAS_FCST {options replace=true} as
	select  pbo.product_id
		, cast(intnx('month', pbo.SALES_DT, 0, 'B')	as date) as month_dt
		, sum(pbo.FINAL_FCST_UNITS_ML      ) as UNITS_SAS_FCST_ML
		, sum(pbo.FINAL_FCST_UNITS_REC_BPLM) as UNITS_SAS_FCST_REC_BPLM
		, sum(pbo.FINAL_FCST_UNITS_REC_APLM) as UNITS_SAS_FCST_REC_APLM
	from &lmvTableFcst. as pbo
	inner join CASUSER.PBO_LIST as loc
		on pbo.PBO_LOCATION_ID = loc.PBO_LOCATION_ID
	inner join CASUSER.PROD_LIST as sku
		on pbo.product_id = sku.product_id
	where   pbo.sales_dt >= &lmvStartDateFormatted. 						/* Фильтр на тестовый период */
		and pbo.sales_dt <= &lmvEndDateFormatted.
	group by 1,2
	;
	/* GC_FCST */
	create table CASUSER.GC_SAS_FCST {options replace=true} as
	select sum(pbo.GC_FCST) as GC_SAS_FCST
	from &lmvTableFcstGc. as pbo
	inner join CASUSER.PBO_LIST as loc
		on pbo.PBO_LOCATION_ID = loc.PBO_LOCATION_ID
	where   pbo.SALES_DT >= &lmvStartDateFormatted. 						/* Фильтр на тестовый период */
		and pbo.SALES_DT <= &lmvEndDateFormatted.							
		and pbo.CHANNEL_CD = 'ALL'											/* Фильтр на канал !!! */
	;
	/* UPT_FCST */
	create table CASUSER.UNITS_N_GC_SAS_FCST {options replace=true} as
	select units.product_id
		, units.month_dt
		, units.UNITS_SAS_FCST_ML
		, units.UNITS_SAS_FCST_REC_BPLM
		, units.UNITS_SAS_FCST_REC_APLM
		, gc.GC_SAS_FCST
		, divide(1000 * units.UNITS_SAS_FCST_ML	     , gc.GC_SAS_FCST) as UPT_SAS_FCST_ML	     
		, divide(1000 * units.UNITS_SAS_FCST_REC_BPLM, gc.GC_SAS_FCST) as UPT_SAS_FCST_REC_BPLM
		, divide(1000 * units.UNITS_SAS_FCST_REC_APLM, gc.GC_SAS_FCST) as UPT_SAS_FCST_REC_APLM
	from CASUSER.UNITS_SAS_FCST as units
	cross join CASUSER.GC_SAS_FCST as gc
	;
quit;



/*
proc fedsql sessref=casauto;
	create table CASUSER.SALE_MCD_FCST {options replace=true} as
	select pbo.PBO_LOCATION_ID
		, pbo.product_id
		, pbo.period_dt as SALES_DT
		, pbo.FINAL_FCST_SALE * rand('uniform', 0.98, 1.02) as SALE_MCD_FCST

	from 	
		&lmvTableFcst. as pbo

	where   pbo.period_dt >= &lmvStartDateFormatted. 						
		and pbo.period_dt <= &lmvEndDateFormatted.							
	;
quit;
*/



proc fedsql sessref=casauto;
	create table CASUSER.SALE_FCST_VS_ACT {options replace=true} as
	select 
		  sas.product_id
		, sas.month_dt

		, sas.UNITS_SAS_FCST_ML
		, sas.UNITS_SAS_FCST_REC_BPLM
		, sas.UNITS_SAS_FCST_REC_APLM
		, sas.GC_SAS_FCST
		, sas.UPT_SAS_FCST_ML	     
		, sas.UPT_SAS_FCST_REC_BPLM
		, sas.UPT_SAS_FCST_REC_APLM

		, coalesce(act.UNITS_ACT, 0) as UNITS_ACT
		, coalesce(act.GC_ACT   , 0) as GC_ACT   
		, coalesce(act.UPT_ACT  , 0) as UPT_ACT  
	

/* 		, mcd.SALE_MCD_FCST */
 
	from CASUSER.UNITS_N_GC_SAS_FCST  as sas

	left join CASUSER.UNITS_N_GC_ACT as act	
		on sas.product_id 		= act.product_id
		and sas.month_dt 		= act.month_dt

/* 	left join CASUSER.SALE_MCD_FCST as act	 */
/* 		on  sas.PBO_LOCATION_ID = act.PBO_LOCATION_ID */
/* 		and sas.product_id 		= act.product_id */
/* 		and sas.SALES_DT 		= act.SALES_DT */

	;
quit;




/**************************************************************************************/
/**************************** ACCURACY CALCULATION ************************************/
/**************************************************************************************/


/* SALE aggregatinng to atomic level:
	- PBO 	  / month
	- Country / month
	- Country / week
*/


proc fedsql sessref=casauto;
	create table casuser.ATOM_PBO_SKU_DAY {options replace=true} as
	select *
		, (UPT_SAS_FCST_ML 	 		- UPT_ACT) AS UPT_SAS_ERR_ML 		
		, (UPT_SAS_FCST_REC_BPLM 	- UPT_ACT) AS UPT_SAS_ERR_REC_BPLM
		, (UPT_SAS_FCST_REC_APLM 	- UPT_ACT) AS UPT_SAS_ERR_REC_APLM
		
		, abs(UPT_SAS_FCST_ML 	 		- UPT_ACT) AS UPT_SAS_ABSERR_ML 		
		, abs(UPT_SAS_FCST_REC_BPLM 	- UPT_ACT) AS UPT_SAS_ABSERR_REC_BPLM
		, abs(UPT_SAS_FCST_REC_APLM 	- UPT_ACT) AS UPT_SAS_ABSERR_REC_APLM
		
/* 		, (SALE_mcd_fcst - SALE_act) as SALE_mcd_err */
/* 		, abs(SALE_mcd_fcst - SALE_act) as SALE_mcd_abserr */
	from
		casuser.SALE_FCST_VS_ACT 
/* 	where flag_filter = 1 */
/* 		and SALE_sas_fcst > 0 */
/* 		and SALE_act > 0  */
	;
quit;

/* KPI calculation */

proc fedsql sessref=casauto;
	create table casuser.KPI_MONTH {options replace=true} as
	select month_dt
	
		, sum(UPT_SAS_ABSERR_ML 		) / sum(UPT_ACT ) as WAPE_SAS_UPT_ML 		
		, sum(UPT_SAS_ABSERR_REC_BPLM 	) / sum(UPT_ACT ) as WAPE_SAS_UPT_REC_BPLM
		, sum(UPT_SAS_ABSERR_REC_APLM 	) / sum(UPT_ACT ) as WAPE_SAS_UPT_REC_APLM
			
/* 		, sum(SALE_mcd_abserr ) / sum(SALE_act ) as WAPE_MCD */

		, sum(UPT_SAS_ERR_ML 		    ) / sum(UPT_ACT ) as BIAS_SAS_UPT_ML 		
		, sum(UPT_SAS_ERR_REC_BPLM    	) / sum(UPT_ACT ) as BIAS_SAS_UPT_REC_BPLM
		, sum(UPT_SAS_ERR_REC_APLM    	) / sum(UPT_ACT ) as BIAS_SAS_UPT_REC_APLM
		
/* 		, sum(SALE_mcd_err    ) / sum(SALE_act ) as BIAS_MCD */

		, sum(UPT_ACT ) as SUM_UPT_ACT

		, sum(UPT_SAS_FCST_ML 		) as SUM_UPT_SAS_FCST_ML 		
		, sum(UPT_SAS_FCST_REC_BPLM ) as SUM_UPT_SAS_FCST_REC_BPLM
		, sum(UPT_SAS_FCST_REC_APLM ) as SUM_UPT_SAS_FCST_REC_APLM
		
	
		
/* 		, sum(SALE_mcd_fcst) as sum_SALE_mcd_fcst */
	
		, sum(UPT_SAS_ABSERR_ML 		 ) as SUM_UPT_SAS_ABSERR_ML 		
		, sum(UPT_SAS_ABSERR_REC_BPLM 	 ) as SUM_UPT_SAS_ABSERR_REC_BPLM 	
		, sum(UPT_SAS_ABSERR_REC_APLM 	 ) as SUM_UPT_SAS_ABSERR_REC_APLM 
		
/* 		, sum(UPT_mcd_abserr ) as sum_SALE_mcd_abserr */

		, sum(UPT_SAS_ERR_ML 		) as SUM_UPT_SAS_ERR_ML 		
		, sum(UPT_SAS_ERR_REC_BPLM  ) as SUM_UPT_SAS_ERR_REC_BPLM 	
		, sum(UPT_SAS_ERR_REC_APLM  ) as SUM_UPT_SAS_ERR_REC_APLM 
		
	
/* 		, sum(SALE_mcd_err    ) as sum_SALE_mcd_err */
		
	from
		casuser.ATOM_PBO_SKU_DAY
	group by month_dt
	;
quit;


data WORK.KPI_MONTH_&lmvOutTablePostfix.;
	set casuser.KPI_MONTH;
	format
		WAPE_SAS_UPT_ML 		PERCENTN8.2
		WAPE_SAS_UPT_REC_BPLM	PERCENTN8.2
		WAPE_SAS_UPT_REC_APLM	PERCENTN8.2
		BIAS_SAS_UPT_ML 		PERCENTN8.2
		BIAS_SAS_UPT_REC_BPLM	PERCENTN8.2
		BIAS_SAS_UPT_REC_APLM	PERCENTN8.2
		SUM_UPT_ACT 					COMMAX15.
		SUM_UPT_SAS_FCST_ML 	 		COMMAX15.
		SUM_UPT_SAS_FCST_REC_BPLM 		COMMAX15.
		SUM_UPT_SAS_FCST_REC_APLM 		COMMAX15.
		SUM_UPT_SAS_ABSERR_ML 	 		COMMAX15.
		SUM_UPT_SAS_ABSERR_REC_BPLM 	COMMAX15.
		SUM_UPT_SAS_ABSERR_REC_APLM	COMMAX15.
		SUM_UPT_SAS_ERR_ML 	 		COMMAX15.
		SUM_UPT_SAS_ERR_REC_BPLM 		COMMAX15.
		SUM_UPT_SAS_ERR_REC_APLM 		COMMAX15.
		/*
		WAPE_MCD			PERCENTN8.2
		BIAS_MCD			PERCENTN8.2
		sum_SALE_mcd_fcst 	COMMAX15.
		sum_SALE_mcd_abserr COMMAX15.
		sum_SALE_mcd_err  	COMMAX15.
		*/
		
	;
run;


/*

%let common_path = /opt/sas/mcd_config/macro/step/pt/short_term;

ods excel file="&common_path./KPI_UPT_&lmvOutTablePostfix..xlsx"  style=statistical;

ods excel options(sheet_interval = 'none' sheet_name = "KPI_UPT_MONTH"	);
proc print data = WORK.KPI_MONTH_&lmvOutTablePostfix. 	label; run;


ods excel close;
