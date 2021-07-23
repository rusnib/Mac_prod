%macro assign;
%let casauto_ok = %sysfunc(SESSFOUND ( cmasauto)) ;
%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
 cas casauto;
 caslib _all_ assign;
%end;
%mend;
%assign

options casdatalimit=600000M;

/* libname ETL_STG "/data2/etl_stg_23_11_2020"; */
/* libname tmp "/data2/TMP"; */
/* libname MCD_CMP "/data2/MCD_CMP"; */
/*  */
/* %let inlib=ETL_STG; */
/* %let mclib=MCD_CMP; */



/* Изменяемые параметры */
/*
01MAR2021		31MAR2021
01DEC2020		31DEC2020
01JAN2021		31JAN2021
*/
%let lmvStartDate 	= '01JAN2021'd;
%let lmvEndDate 	= '31JAN2021'd;

%let lmvTableFcstGc = MAX_CASL.GC_FORECAST_RESTORED_JAN_2;
%let lmvOutTablePostfix = JAN;


/* Неизменяемые параметры */
%let lmvReportDttm 	       = &ETL_CURRENT_DTTM.;
%let lmvStartDateFormatted = %str(date%')%sysfunc(putn(&lmvStartDate., yymmdd10.))%str(%');
%let lmvEndDateFormatted   = %str(date%')%sysfunc(putn(&lmvEndDate.  , yymmdd10.))%str(%');
%let lmvTestMonthDate 	   = %str(date%')%sysfunc(putn(%sysfunc(intnx(month,&lmvStartDate.,0)), yymmdd10.))%str(%');

/* Поднятие в CAS истории чеков */
/* data CASUSER.PBO_SALES (replace=yes drop=valid_from_dttm valid_to_dttm); */
/* 	set ETL_IA.PBO_SALES (where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.)); */
/* run; */


/************************************************************************************/
/******************************* 2.1 Restaurants list *******************************/
/************************************************************************************/

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
/* !!! Note, that it's more strong condition, which includes: 
		- having sales on test period
		- no temporary closings on test period 
*/
proc fedsql sessref=casauto;
	create table CASUSER.PBO_LIST_GC_OVER100 {options replace=true} as
	select PBO_LOCATION_ID
		, count(SALES_DT) as count_days 								/* Кол-во дней после фильтрации */
	from CASUSER.PBO_SALES 
	where RECEIPT_QTY > 100												/* Фильтр на 100 чеков */
		and SALES_DT >= &lmvStartDateFormatted. 						/* Фильтр на тестовый период */
		and SALES_DT <= &lmvEndDateFormatted.							
		and CHANNEL_CD = 'ALL'											/* Фильтр на канал !!! */
	group by PBO_LOCATION_ID
	/* Проверяем, что после фильтрации кол-во осташихся дней продаж равно кол-во дней в тестовом месяце */
	having count(SALES_DT) = 	1 + intck('day', &lmvStartDateFormatted., &lmvEndDateFormatted.)
	/* После применения фильтра Having: 
		- На марте 2021: 	788 -> 754 
		- На январе 2021: 	784 -> 700
		- На декабре 2020: 	783 -> 720
	*/
	;
quit;



/**************************************************************************************/
/*********************** ACTUAL & FORECAST DATA PREPARATION ***************************/
/**************************************************************************************/


proc fedsql sessref=casauto;
	create table CASUSER.GC_ACT {options replace=true} as
	select pbo.PBO_LOCATION_ID
		, pbo.SALES_DT
		, pbo.RECEIPT_QTY as GC_ACT
	
	from CASUSER.PBO_SALES as pbo
	
	inner join CASUSER.PBO_LIST_COMP as comp
		on pbo.PBO_LOCATION_ID = comp.PBO_LOCATION_ID
	
	inner join CASUSER.PBO_LIST_GC_OVER100 as o100
		on pbo.PBO_LOCATION_ID = o100.PBO_LOCATION_ID
	
	where   pbo.SALES_DT >= &lmvStartDateFormatted. 						/* Фильтр на тестовый период */
		and pbo.SALES_DT <= &lmvEndDateFormatted.							
		and pbo.CHANNEL_CD = 'ALL'											/* Фильтр на канал !!! */
	;
quit;

proc fedsql sessref=casauto;
	create table CASUSER.GC_SAS_FCST {options replace=true} as
	select pbo.PBO_LOCATION_ID
		, pbo.SALES_DT
		, pbo.GC_FCST as GC_SAS_FCST

	from &lmvTableFcstGc. as pbo

	inner join CASUSER.PBO_LIST_COMP as comp
		on pbo.PBO_LOCATION_ID = comp.PBO_LOCATION_ID

	inner join CASUSER.PBO_LIST_GC_OVER100 as o100
		on pbo.PBO_LOCATION_ID = o100.PBO_LOCATION_ID

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

	inner join CASUSER.PBO_LIST_COMP as comp
		on pbo.PBO_LOCATION_ID = comp.PBO_LOCATION_ID

	inner join CASUSER.PBO_LIST_GC_OVER100 as o100
		on pbo.PBO_LOCATION_ID = o100.PBO_LOCATION_ID

	where   pbo.SALES_DT >= &lmvStartDateFormatted. 						/* Фильтр на тестовый период */
		and pbo.SALES_DT <= &lmvEndDateFormatted.							
		and pbo.CHANNEL_CD = 'ALL'											/* Фильтр на канал !!! */
	;
quit;

proc fedsql sessref=casauto;
	create table CASUSER.GC_FCST_VS_ACT {options replace=true} as
	select act.PBO_LOCATION_ID
		, act.SALES_DT
		, act.GC_ACT
		, sas.GC_SAS_FCST
		, mcd.GC_MCD_FCST
 
	from CASUSER.GC_ACT  as act

	inner join CASUSER.GC_SAS_FCST as sas	
		on  act.PBO_LOCATION_ID = sas.PBO_LOCATION_ID
		and act.SALES_DT 		= sas.SALES_DT

	inner join CASUSER.GC_MCD_FCST as mcd	
		on  act.PBO_LOCATION_ID = mcd.PBO_LOCATION_ID
		and act.SALES_DT 		= mcd.SALES_DT
	;
quit;


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
/* 		, intnx('month', SALES_DT, 0, 'B') as month_dt */
		, intnx('week.2', SALES_DT, 0, 'B') as week_dt
		, (gc_sas_fcst - gc_act) as gc_sas_err
		, (gc_mcd_fcst - gc_act) as gc_mcd_err
		, abs(gc_sas_fcst - gc_act) as gc_sas_abserr
		, abs(gc_mcd_fcst - gc_act) as gc_mcd_abserr
	from
		casuser.GC_FCST_VS_ACT 
	;
quit;

proc fedsql sessref=casauto;
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


%let common_path = /opt/sas/mcd_config/macro/step/pt/short_term;

ods excel file="&common_path./KPI_GC_&lmvOutTablePostfix..xlsx"  style=statistical;

ods excel options(sheet_interval = 'none' sheet_name = "KPI_GC_WEEK"	);
proc print data = WORK.KPI_WEEK_&lmvOutTablePostfix. 	label; run;

ods excel options(sheet_interval = 'proc' sheet_name = "KPI_GC_MONTH"	);
proc print data = WORK.KPI_MONTH_&lmvOutTablePostfix.	label; run;

ods excel close;
