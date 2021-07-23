%macro assign;
%let casauto_ok = %sysfunc(SESSFOUND ( cmasauto)) ;
%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
 cas casauto;
 caslib _all_ assign;
%end;
%mend;
%assign

options casdatalimit=600000M;



/* Изменяемые параметры */
/*
01MAR2021		31MAR2021
01DEC2020		31DEC2020
01JAN2021		31JAN2021
*/
%let lmvStartDate 	= '01MAR2021'd;
%let lmvEndDate 	= '31MAR2021'd;

%let lmvTableFcstGc 	= MAX_CASL.GC_FORECAST_RESTORED_MAR;
%let lmvOutTablePostfix = MAR;
%let lmvCasLibLaunch	= MN_SHORT; /* MN_SHORT or CASUSER */

/* Неизменяемые параметры */
%let lmvReportDttm 	       = &ETL_CURRENT_DTTM.;
%let lmvStartDateFormatted = %str(date%')%sysfunc(putn(&lmvStartDate., yymmdd10.))%str(%');
%let lmvEndDateFormatted   = %str(date%')%sysfunc(putn(&lmvEndDate.  , yymmdd10.))%str(%');
%let lmvTestMonthDate 	   = %str(date%')%sysfunc(putn(%sysfunc(intnx(month,&lmvStartDate.,0)), yymmdd10.))%str(%');
%let lmvExcludingList 	   = 9908, 1494, 1495, 1496, 1497, 1498, 1499 ;

/* Поднятие в CAS истории чеков */
data CASUSER.PBO_SALES (replace=yes drop=valid_from_dttm valid_to_dttm);
	set ETL_IA.PBO_SALES (where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
run;

/* Поднятие в CAS истории продаж и справочников*/
%macro load_to_cas;
	%if &lmvCasLibLaunch. = CASUSER %then %do;
		/* PMIX_SALES */
		data CASUSER.PMIX_SALES (replace=yes drop=valid_from_dttm valid_to_dttm);
			set ETL_IA.PMIX_SALES (where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
		run;
		/* PBO_CLOSE_PERIOD */
		data CASUSER.PBO_CLOSE_PERIOD (replace=yes drop=valid_from_dttm valid_to_dttm);
			set ETL_IA.PBO_CLOSE_PERIOD (where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
		run;
		/* DICTIONARIES */
		%let common_path = /opt/sas/mcd_config/macro/step/pt/alerts;
		%include "&common_path./data_prep_product.sas"; 
		%include "&common_path./data_prep_pbo.sas"; 
		%data_prep_product(
			  mpInLib 		= ETL_IA
			, mpReportDttm 	= &ETL_CURRENT_DTTM.
			, mpOutCasTable = CASUSER.PRODUCT_DICTIONARY
		);
		%data_prep_pbo(
			  mpInLib 		= ETL_IA
			, mpReportDttm 	= &ETL_CURRENT_DTTM.
			, mpOutCasTable = CASUSER.PBO_DICTIONARY
		);
	%end;
%mend load_to_cas;
%load_to_cas;

/************************************************************************************/
/******************************* 2.1 Restaurants list *******************************/
/************************************************************************************/

/* Step 0. Closed dates */

/* ------------ Start. Дни когда пбо будет уже закрыт (навсегда) ------------------ */
	data casuser.days_pbo_date_close;
		set &lmvCasLibLaunch..PBO_DICTIONARY;
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
		set &lmvCasLibLaunch..PBO_CLOSE_PERIOD;
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
	&lmvCasLibLaunch..PBO_DICTIONARY

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
	from &lmvCasLibLaunch..PMIX_SALES
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




/**************************************************************************************/
/*********************** ACTUAL & FORECAST DATA PREPARATION ***************************/
/**************************************************************************************/


proc fedsql sessref=casauto;
	create table CASUSER.GC_SAS_FCST {options replace=true} as
	select pbo.PBO_LOCATION_ID
		, pbo.SALES_DT
		, pbo.GC_FCST as GC_SAS_FCST

	from &lmvTableFcstGc. as pbo

	inner join CASUSER.PBO_LIST as list
		on pbo.PBO_LOCATION_ID = list.PBO_LOCATION_ID

	where   pbo.SALES_DT >= &lmvStartDateFormatted. 						/* Фильтр на тестовый период */
		and pbo.SALES_DT <= &lmvEndDateFormatted.							
		and pbo.CHANNEL_CD = 'ALL'											/* Фильтр на канал !!! */
	;
quit;

proc fedsql sessref=casauto;
	create table CASUSER.GC_ACT {options replace=true} as
	select pbo.PBO_LOCATION_ID
		, pbo.SALES_DT
		, pbo.RECEIPT_QTY as GC_ACT
	
	from CASUSER.PBO_SALES as pbo
	
	where   pbo.SALES_DT >= &lmvStartDateFormatted. 						/* Фильтр на тестовый период */
		and pbo.SALES_DT <= &lmvEndDateFormatted.							
		and pbo.CHANNEL_CD = 'ALL'											/* Фильтр на канал !!! */
	;
quit;

proc fedsql sessref=casauto;
	create table CASUSER.GC_MCD_FCST {options replace=true} as
	select pbo.PBO_LOCATION_ID
		, pbo.SALES_DT
		, pbo.GC_FCST * rand('uniform', 0.98, 1.02) as GC_MCD_FCST

	from 	/* ВСТАВИТЬ ПОДГРУЗКУ ПЛАНОВ McD */
		&lmvTableFcstGc. as pbo

	where   pbo.SALES_DT >= &lmvStartDateFormatted. 						/* Фильтр на тестовый период */
		and pbo.SALES_DT <= &lmvEndDateFormatted.							
		and pbo.CHANNEL_CD = 'ALL'											/* Фильтр на канал !!! */
	;
quit;

proc fedsql sessref=casauto;
	create table CASUSER.GC_FCST_VS_ACT {options replace=true} as
	select sas.PBO_LOCATION_ID
		, sas.SALES_DT
		, act.GC_ACT
		, sas.GC_SAS_FCST
		, mcd.GC_MCD_FCST
 
	from CASUSER.GC_SAS_FCST  as sas

	left join CASUSER.GC_ACT as act	
		on  sas.PBO_LOCATION_ID = act.PBO_LOCATION_ID
		and sas.SALES_DT 		= act.SALES_DT

	left join CASUSER.GC_MCD_FCST as mcd	
		on  sas.PBO_LOCATION_ID = mcd.PBO_LOCATION_ID
		and sas.SALES_DT 		= mcd.SALES_DT
	;
quit;

proc casutil;
	droptable 
		casdata		= "GC_FCST_VS_ACT_&lmvOutTablePostfix." 
		incaslib	= "MAX_CASL" 
		quiet         
	;                 
run;  

data MAX_CASL.GC_FCST_VS_ACT_&lmvOutTablePostfix.;
	set CASUSER.GC_FCST_VS_ACT;
run;

proc casutil;         
	promote           
		casdata		= "GC_FCST_VS_ACT_&lmvOutTablePostfix." 
		incaslib	= "MAX_CASL" 
		casout		= "GC_FCST_VS_ACT_&lmvOutTablePostfix."  
		outcaslib	= "MAX_CASL"
	;                 
run;  



/**************************************************************************************/
/**************************** ACCURACY CALCULATION ************************************/
/**************************************************************************************/


/* GC aggregatinng to atomic level:
	- PBO 	  / month
	- Country / month
	- Country / week
*/

proc fedsql sessref=casauto;
	create table casuser.ATOM_PBO_DAY {options replace=true} as
	select *
		, intnx('week.2', SALES_DT, 0, 'B') as week_dt
		, (gc_sas_fcst - gc_act) as gc_sas_err
		, (gc_mcd_fcst - gc_act) as gc_mcd_err
		, abs(gc_sas_fcst - gc_act) as gc_sas_abserr
		, abs(gc_mcd_fcst - gc_act) as gc_mcd_abserr
	from
		casuser.GC_FCST_VS_ACT 
	;
	create table casuser.ATOM_PBO_MONTH {options replace=true} as
	select PBO_LOCATION_ID
		, intnx('month', SALES_DT, 0, 'B') as month_dt
		, sum(gc_act ) as gc_act

		, sum(gc_sas_fcst) as gc_sas_fcst
		, sum(gc_mcd_fcst) as gc_mcd_fcst
		
		, (sum(gc_sas_fcst) - sum(gc_act)) as gc_sas_err
		, (sum(gc_mcd_fcst) - sum(gc_act)) as gc_mcd_err
		
		, abs(sum(gc_sas_fcst) - sum(gc_act)) as gc_sas_abserr
		, abs(sum(gc_mcd_fcst) - sum(gc_act)) as gc_mcd_abserr
	from
		casuser.GC_FCST_VS_ACT 
	group by 1,2
	;
quit;


/* KPI calculation */

proc fedsql sessref=casauto;
	create table casuser.KPI_MONTH {options replace=true} as
	select month_dt
	
		, sum(gc_sas_abserr ) / sum(gc_act ) as WAPE_SAS
		, sum(gc_mcd_abserr ) / sum(gc_act ) as WAPE_MCD

		, sum(gc_sas_err    ) / sum(gc_act ) as BIAS_SAS
		, sum(gc_mcd_err    ) / sum(gc_act ) as BIAS_MCD

		, sum(gc_act ) as sum_gc_act

		, sum(gc_sas_fcst) as sum_gc_sas_fcst
		, sum(gc_mcd_fcst) as sum_gc_mcd_fcst
	
		, sum(gc_sas_abserr ) as sum_gc_sas_abserr
		, sum(gc_mcd_abserr ) as sum_gc_mcd_abserr
		
		, sum(gc_sas_err    ) as sum_gc_sas_err
		, sum(gc_mcd_err    ) as sum_gc_mcd_err
		
	from
		casuser.ATOM_PBO_MONTH 
	group by month_dt
	;
quit;

data WORK.KPI_MONTH_&lmvOutTablePostfix.;
	set casuser.KPI_MONTH;
	format
		month_dt 			date9.
		WAPE_SAS			PERCENTN8.2
		WAPE_MCD			PERCENTN8.2
		BIAS_SAS			PERCENTN8.2
		BIAS_MCD			PERCENTN8.2
		sum_gc_act 			COMMAX15.
		sum_gc_sas_fcst 	COMMAX15.
		sum_gc_mcd_fcst 	COMMAX15.
		sum_gc_sas_abserr 	COMMAX15.
		sum_gc_mcd_abserr  	COMMAX15.
		sum_gc_sas_err 		COMMAX15.
		sum_gc_mcd_err  	COMMAX15.
	;
run;


proc fedsql sessref=casauto;
	create table casuser.KPI_WEEK {options replace=true} as
	select week_dt
	
		, sum(gc_sas_abserr ) / sum(gc_act ) as WAPE_SAS
		, sum(gc_mcd_abserr ) / sum(gc_act ) as WAPE_MCD

		, sum(gc_sas_err    ) / sum(gc_act ) as BIAS_SAS
		, sum(gc_mcd_err    ) / sum(gc_act ) as BIAS_MCD

		, sum(gc_act ) as sum_gc_act

		, sum(gc_sas_fcst) as sum_gc_sas_fcst
		, sum(gc_mcd_fcst) as sum_gc_mcd_fcst
	
		, sum(gc_sas_abserr ) as sum_gc_sas_abserr
		, sum(gc_mcd_abserr ) as sum_gc_mcd_abserr
		
		, sum(gc_sas_err    ) as sum_gc_sas_err
		, sum(gc_mcd_err    ) as sum_gc_mcd_err
		
	from
		casuser.ATOM_PBO_DAY 
	group by week_dt
	;
quit;

data WORK.KPI_WEEK_&lmvOutTablePostfix.;
	set casuser.KPI_WEEK;
	format
		week_dt 			date9.
		WAPE_SAS			PERCENTN8.2
		WAPE_MCD			PERCENTN8.2
		BIAS_SAS			PERCENTN8.2
		BIAS_MCD			PERCENTN8.2
		sum_gc_act 			COMMAX15.
		sum_gc_sas_fcst 	COMMAX15.
		sum_gc_mcd_fcst 	COMMAX15.
		sum_gc_sas_abserr 	COMMAX15.
		sum_gc_mcd_abserr  	COMMAX15.
		sum_gc_sas_err 		COMMAX15.
		sum_gc_mcd_err  	COMMAX15.
	;
run;

/*

%let common_path = /opt/sas/mcd_config/macro/step/pt/short_term;

ods excel file="&common_path./KPI_GC_&lmvOutTablePostfix..xlsx"  style=statistical;

ods excel options(sheet_interval = 'none' sheet_name = "KPI_GC_WEEK"	);
proc print data = WORK.KPI_WEEK_&lmvOutTablePostfix. 	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "KPI_GC_MONTH"	);
proc print data = WORK.KPI_MONTH_&lmvOutTablePostfix.	label; run;

ods excel close;
