/* ****************** */
/* Макрос для построения витрин GC и PBO в рамках сквозного процесса прогнозирования */
/* Параметры 1. mpMode  GC | PBO */
/*  		 2. mpOutTableDmVf - имя выходной таблицы в двухуровневом формате */
/* ****************** */
/* 
	Пример использования: fcst_create_abt_pbo_gc(mpMode=pbo
							 ,mpOutTableDmVf = MN_DICT.PBO_SM_TRAIN_TRP
							 ,mpOutTableDmABT = MN_DICT.TRAIN_ABT_TRP
							 );
*/
%macro fcst_create_abt_pbo_gc(mpMode=pbo
							 ,mpOutTableDmVf = MN_DICT.PBO_SM_TRAIN_TRP
							 ,mpOutTableDmABT = MN_DICT.TRAIN_ABT_TRP
							 );
							
	%local 	lmvMode
			lmvReportDttm
			lmvLibrefOut
			lmvTabNmOut
			lmvLibrefOutABT
			lmvTabNmOutABT
			lmvInLib
	;
	
	%let lmvInLib = ETL_IA;
	
	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						);
	
	/* Подтягиваем данные из PROMOTOOL */
	%add_promotool_marks2(mpOutCaslib=casuser,
							mpPtCaslib=pt);
							
	
	%member_names (mpTable=&mpOutTableDmVf, mpLibrefNameKey=lmvLibrefOut, mpMemberNameKey=lmvTabNmOut);
	%member_names (mpTable=&mpOutTableDmABT, mpLibrefNameKey=lmvLibrefOutABT, mpMemberNameKey=lmvTabNmOutABT);
	
	%let lmvMode = %upcase(&mpMode.);
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	%let start_date = &ETL_CURRENT_DT.;
	%let end_date = %sysfunc(intnx(day,&ETL_CURRENT_DT.,92));
	%let fcst_start_dt =  %str(date%')%sysfunc(putn(&ETL_CURRENT_DT., yymmdd10.))%str(%');
	%let history_end_dt = %str(date%')%sysfunc(putn(%sysfunc(intnx(day,&ETL_CURRENT_DT.,-1)), yymmdd10.))%str(%');
	%let sas_history_end_dt = %sysfunc(intnx(day,&ETL_CURRENT_DT.,-1));

	%if &lmvMode. = GC %then %do;
		/* 1. Timeseries MA[7] */
		/* GC mode */
		PROC SQL noprint;
		   CREATE TABLE work.PBO_SALES AS 
		   SELECT t1.PBO_LOCATION_ID, 
				  t1.CHANNEL_CD, 
				  t1.RECEIPT_QTY, 
				  t1.SALES_DT
			  FROM ETL_IA.pbo_sales t1
			  where valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
		;
		QUIT;
		
		PROC SORT
			DATA=WORK.pbo_sales
			OUT=WORK.TMP0TempTableInput
			;
			BY PBO_LOCATION_ID CHANNEL_CD SALES_DT;
		RUN;
		/* KEEP=SALES_DT RECEIPT_QTY PBO_LOCATION_ID CHANNEL_CD */
		/* end GC mode */
	%end;
	%else %if &lmvMode. = PBO %then %do;
		/* PBO mode */
		PROC SQL noprint;
		   CREATE TABLE work.PMIX_PBO_AGGR AS 
		   SELECT t1.PBO_LOCATION_ID, 
				  t1.CHANNEL_CD, 
				  sum(sum(t1.SALES_QTY, t1.SALES_QTY_PROMO)) as RECEIPT_QTY, 
				  t1.SALES_DT
			  FROM ETL_IA.pmix_sales t1
			  where valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.
			  GROUP BY 
				  t1.CHANNEL_CD,
				  t1.PBO_LOCATION_ID,
				  t1.SALES_DT
		;
		QUIT;
		
		PROC SORT
			DATA=work.PMIX_PBO_AGGR(KEEP=SALES_DT RECEIPT_QTY PBO_LOCATION_ID CHANNEL_CD)
			OUT=WORK.TMP0TempTableInput
			;
			BY PBO_LOCATION_ID CHANNEL_CD SALES_DT;
		RUN;
		/* end PBO mode */
	%end;
	PROC EXPAND DATA=WORK.TMP0TempTableInput
		OUT=casuser.MA7_TIMESERIES
		ALIGN = BEGINNING
		METHOD = SPLINE(NOTAKNOT, NOTAKNOT) 
		OBSERVED = (BEGINNING, BEGINNING) 
	;

		BY PBO_LOCATION_ID CHANNEL_CD;
		ID SALES_DT;
		CONVERT RECEIPT_QTY = new_RECEIPT_QTY / 
			TRANSFORMIN	= (CMOVAVE  7)
				
			;
	RUN;

	/* 2. Comp PBO */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.MA7_TIMESERIES_CMP{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t1.CHANNEL_CD, 
			  t1.new_RECEIPT_QTY, 
			  t1.RECEIPT_QTY, 
			  /* SALES_DT */
				(DATEPART(t1.SALES_DT)) AS SALES_DT
		  FROM casuser.MA7_TIMESERIES t1
		  WHERE DATEPART(t1.SALES_DT) <= &history_end_dt.
	;
	QUIT;

	data casuser.dates;
		do SALES_DT=&start_date. to &end_date.;
		new_RECEIPT_QTY = .;
		RECEIPT_QTY = .;
		output;
		end;
		format SALES_DT DDMMYYP.;
	run;

	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.MA7_CMP_DISTINCT{options replace=true} AS 
	   SELECT DISTINCT t1.CHANNEL_CD, 
			  t1.PBO_LOCATION_ID
		  FROM casuser.MA7_TIMESERIES_CMP t1
	;
	QUIT;

	/* 7. */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.FUTURE_SKELETON{options replace=true} AS 
	   SELECT t1.CHANNEL_CD, 
			  t1.PBO_LOCATION_ID, 
			  t2.SALES_DT, 
			  t2.new_RECEIPT_QTY, 
			  t2.RECEIPT_QTY
		  FROM casuser.MA7_CMP_DISTINCT t1
			   CROSS JOIN casuser.DATES t2;
	QUIT;

	/* 8. Append future to history */
	data casuser.SALES_FULL;
		set casuser.MA7_TIMESERIES_CMP casuser.FUTURE_SKELETON;
	run;

	/* 9. Add WOY and DOY and other stuff */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.SALES_WITH_WOY_DOY{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t1.CHANNEL_CD, 
			  t1.new_RECEIPT_QTY, 
			  t1.RECEIPT_QTY, 
			  t1.SALES_DT, 
			  /* WOY */
				(week(t1.SALES_DT, 'w')) AS WOY, 
			  /* DOW */
				(case when weekday(t1.SALES_DT) = 1 then 7 else weekday(t1.SALES_DT) - 1 end) AS DOW, 
			  /* WBY_TEMP */
				(week(MDY(12, 31, YEAR(t1.SALES_DT)), 'w') - (week(t1.SALES_DT, 'w')) + 1) AS WBY_TEMP, 
			  /* EOY */
				(intnx('year', t1.SALES_DT, 0, 'e')) AS EOY, 
			  /* WBY */
				(intck('week.2', t1.SALES_DT, Intnx('year', t1.SALES_DT, 0, 'e'), 'continuous') + 1) AS WBY, 
			  /* LWY */
				(week(MDY(12, 31, YEAR(t1.SALES_DT)), 'w')) AS LWY
		  FROM casuser.SALES_FULL t1
	;
	QUIT;

	/* 10.  */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.PRE_COVID_WOY_DOY{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t1.CHANNEL_CD, 
			  t1.new_RECEIPT_QTY AS new_RECEIPT_QTY_weekly, 
			  t1.RECEIPT_QTY, 
			  t1.SALES_DT, 
			  t1.WOY, 
			  t1.DOW, 
			  t1.WBY
		  FROM casuser.SALES_WITH_WOY_DOY t1
		  WHERE t1.SALES_DT < date '2020-03-01';
	QUIT;

	/* 11.  */
	PROC SORT
		DATA=casuser.PRE_COVID_WOY_DOY(KEEP=SALES_DT RECEIPT_QTY CHANNEL_CD PBO_LOCATION_ID)
		OUT=WORK.TMP0TempTableInput
		;
		BY CHANNEL_CD PBO_LOCATION_ID SALES_DT;
	RUN;

	PROC EXPAND DATA=WORK.TMP0TempTableInput
		OUT=casuser.MA364_TIMESERIES(LABEL="Modified Time Series data for casuser.PRE_COVID_WOY_DOY")
		ALIGN = BEGINNING
		METHOD = SPLINE(NOTAKNOT, NOTAKNOT) 
		OBSERVED = (BEGINNING, BEGINNING) 
	;

		BY CHANNEL_CD PBO_LOCATION_ID;
		ID SALES_DT;
		CONVERT RECEIPT_QTY = new_RECEIPT_QTY / 
			TRANSFORMIN	= (CMOVAVE  364)
				
			;
	RUN;

	/* 12.  */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.MA364_WITH_WOY_DOY{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t1.CHANNEL_CD, 
			  t1.new_RECEIPT_QTY_weekly, 
			  t1.RECEIPT_QTY, 
			  t1.SALES_DT, 
			  t1.WOY, 
			  t1.DOW, 
			  t1.WBY, 
			  t2.new_RECEIPT_QTY AS new_RECEIPT_QTY_yearly
		  FROM casuser.PRE_COVID_WOY_DOY t1, casuser.MA364_TIMESERIES t2
		  WHERE (t1.CHANNEL_CD = t2.CHANNEL_CD AND t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID AND t1.SALES_DT = t2.SALES_DT);
	QUIT;

	/* 13.  */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.MA364_DETREND_DESEASON{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t1.CHANNEL_CD, 
			  t1.new_RECEIPT_QTY_weekly, 
			  t1.RECEIPT_QTY, 
			  t1.SALES_DT, 
			  t1.WOY, 
			  t1.DOW, 
			  t1.WBY, 
			  t1.new_RECEIPT_QTY_yearly, 
			  /* Detrend_multi */
				(t1.RECEIPT_QTY / t1.new_RECEIPT_QTY_yearly) AS Detrend_multi, 
			  /* Detrend_sm_multi */
				(t1.new_RECEIPT_QTY_weekly / t1.new_RECEIPT_QTY_yearly) AS Detrend_sm_multi 
		  FROM casuser.MA364_WITH_WOY_DOY t1
		  WHERE t1.SALES_DT >= date '2017-07-05' AND t1.SALES_DT <= date '2019-09-15';
	QUIT;

	/* 14.  */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.QUERY_FOR_TSDSTIMESERIESOUT_000D{options replace=true} AS 
	   SELECT t1.CHANNEL_CD, 
			  t1.PBO_LOCATION_ID, 
			  t1.WOY, 
			  t1.DOW, 
			  /* AVG_of_Detrend_sm_multi */
				(AVG(t1.Detrend_sm_multi)) AS AVG_of_Detrend_sm_multi, 
			  /* AVG_of_Detrend_multi */
				(AVG(t1.Detrend_multi)) AS AVG_of_Detrend_multi
		  FROM casuser.MA364_DETREND_DESEASON t1
		  GROUP BY t1.CHANNEL_CD,
				   t1.PBO_LOCATION_ID,
				   t1.WOY,
				   t1.DOW;
	QUIT;

	/* 15.  */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.QUERY_FOR_TSDSTIMESERIESOUT_0018{options replace=true} AS 
	   SELECT t1.CHANNEL_CD, 
			  t1.PBO_LOCATION_ID, 
			  t1.WBY, 
			  t1.DOW, 
			  /* AVG_of_Detrend_sm_multi */
				(AVG(t1.Detrend_sm_multi)) AS AVG_of_Detrend_sm_multi, 
			  /* AVG_of_Detrend_multi */
				(AVG(t1.Detrend_multi)) AS AVG_of_Detrend_multi 
		  FROM casuser.MA364_DETREND_DESEASON t1
		  GROUP BY t1.CHANNEL_CD,
				   t1.PBO_LOCATION_ID,
				   t1.WBY,
				   t1.DOW;
	QUIT;

	/* 16.  */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.QUERY_FOR_TSDSTIMESERIESOUT_000F{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t1.CHANNEL_CD, 
			  t1.new_RECEIPT_QTY, 
			  t1.RECEIPT_QTY, 
			  t1.SALES_DT, 
			  (INTNX('week.2', t1.SALES_DT, 0, 'b')) AS SALES_WK,
			  t1.WOY, 
			  t1.DOW, 
			  t1.WBY, 
			  t2.AVG_of_Detrend_sm_multi, 
			  t2.AVG_of_Detrend_multi, 
			  t3.AVG_of_Detrend_sm_multi AS AVG_of_Detrend_sm_multi_WBY, 
			  t3.AVG_of_Detrend_multi AS AVG_of_Detrend_multi_WBY
		  FROM casuser.SALES_WITH_WOY_DOY t1
			   LEFT JOIN casuser.QUERY_FOR_TSDSTIMESERIESOUT_000D t2 ON (t1.CHANNEL_CD = t2.CHANNEL_CD) AND 
			  (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID) AND (t1.WOY = t2.WOY) AND (t1.DOW = t2.DOW)
			   LEFT JOIN casuser.QUERY_FOR_TSDSTIMESERIESOUT_0018 t3 ON (t1.CHANNEL_CD = t3.CHANNEL_CD) AND 
			  (t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID) AND (t1.DOW = t3.DOW) AND (t1.WBY = t3.WBY)
	;
	QUIT;

	/* 17.  */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.QUERY_FOR_TSDSTIMESERIESOUT_001D{options replace=true} AS 
	   SELECT t1.CHANNEL_CD, 
			  t1.SALES_DT, 
			  t1.PBO_LOCATION_ID, 
			  t1.WOY, 
			  t1.DOW, 
			  t1.WBY, 
			  t1.AVG_of_Detrend_sm_multi, 
			  t1.AVG_of_Detrend_multi
		  FROM casuser.QUERY_FOR_TSDSTIMESERIESOUT_000F t1
		  WHERE t1.AVG_of_Detrend_sm_multi ^= .;
	QUIT;

	/* 18.  PBO ATTRIBUTES */
	data CASUSER.PBO_LOCATION (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..pbo_location(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.PBO_LOC_HIERARCHY (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..PBO_LOC_HIERARCHY(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	data CASUSER.PBO_LOC_ATTRIBUTES (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..PBO_LOC_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc cas;
	transpose.transpose /
	   table={name="PBO_LOC_ATTRIBUTES", caslib="casuser", groupby={"pbo_location_id"}} 
	   attributes={{name="pbo_location_id"}} 
	   transpose={"PBO_LOC_ATTR_VALUE"} 
	   prefix="" 
	   id={"PBO_LOC_ATTR_NM"} 
	   casout={name="attr_transposed", caslib="casuser", replace=true};
	quit;

	proc fedsql sessref=casauto;
	   create table casuser.pbo_hier_flat{options replace=true} as
			select t1.pbo_location_id, 
				   t2.PBO_LOCATION_ID as LVL3_ID,
				   t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
				   1 as LVL1_ID
			from 
			(select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=4) as t1
			left join 
			(select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=3) as t2
			on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
			;
	quit;

	proc fedsql sessref=casauto;
		create table casuser.pbo_dictionary_ml{options replace=true} as
			select 
				t2.pbo_location_id, 
				coalesce(t2.lvl3_id,-999) as lvl3_id,
				coalesce(t2.lvl2_id,-99) as lvl2_id,
				coalesce(t14.pbo_location_nm,'NA') as pbo_location_nm,
				coalesce(t13.pbo_location_nm,'NA') as lvl3_nm,
				coalesce(t12.pbo_location_nm,'NA') as lvl2_nm,
				t3.AGREEMENT_TYPE,
				t3.BREAKFAST,
				t3.BUILDING_TYPE,
				t3.COMPANY,
				t3.DELIVERY,
				t3.DRIVE_THRU,
				t3.MCCAFE_TYPE,
				t3.PRICE_LEVEL,
				t3.WINDOW_TYPE
			from 
				casuser.pbo_hier_flat t2
			left join
				casuser.attr_transposed t3
			on
				t2.pbo_location_id=t3.pbo_location_id
			left join
				casuser.PBO_LOCATION t14
			on 
				t2.pbo_location_id=t14.pbo_location_id
			left join
				casuser.PBO_LOCATION t13
			on 
				t2.lvl3_id=t13.pbo_location_id
			left join
				casuser.PBO_LOCATION t12
			on
				t2.lvl2_id=t12.pbo_location_id;
	quit;

	/* 19.  */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.REGION_SEASONALITY_WBY{options replace=true} AS 
	   SELECT t2.CHANNEL_CD, 
			  t1.LVL2_ID AS Region, 
			  t1.BUILDING_TYPE, 
			  t2.WBY, 
			  t2.DOW, 
			  /* AVG_of_AVG_of_Detrend_sm_multi */
				(AVG(t2.AVG_of_Detrend_sm_multi)) AS AVG_of_AVG_of_Detrend_sm_multi, 
			  /* AVG_of_AVG_of_Detrend_multi */
				(AVG(t2.AVG_of_Detrend_multi)) AS AVG_of_AVG_of_Detrend_multi
		  FROM casuser.pbo_dictionary_ml t1
			   INNER JOIN casuser.QUERY_FOR_TSDSTIMESERIESOUT_001D t2 ON (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID)
		  GROUP BY t2.CHANNEL_CD,
				   t1.LVL2_ID,
				   t1.BUILDING_TYPE,
				   t2.WBY,
				   t2.DOW;
	QUIT;

	/* 20.  */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.REGION_SEASONALITY{options replace=true} AS 
	   SELECT t2.CHANNEL_CD, 
			  t1.LVL2_ID AS Region, 
			  t1.BUILDING_TYPE, 
			  t2.WOY, 
			  t2.DOW, 
			  /* AVG_of_AVG_of_Detrend_sm_multi */
				(AVG(t2.AVG_of_Detrend_sm_multi)) AS AVG_of_AVG_of_Detrend_sm_multi, 
			  /* AVG_of_AVG_of_Detrend_multi */
				(AVG(t2.AVG_of_Detrend_multi)) AS AVG_of_AVG_of_Detrend_multi
		  FROM casuser.pbo_dictionary_ml t1
			   INNER JOIN casuser.QUERY_FOR_TSDSTIMESERIESOUT_001D t2 ON (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID)
		  GROUP BY t2.CHANNEL_CD,
				   t1.LVL2_ID,
				   t1.BUILDING_TYPE,
				   t2.WOY,
				   t2.DOW;
	QUIT;

	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.REGION_SEASONALITY_0001{options replace=true} AS 
	   SELECT t2.CHANNEL_CD, 
			  t1.BUILDING_TYPE, 
			  t2.WOY, 
			  t2.DOW, 
			  /* AVG_of_AVG_of_Detrend_sm_multi */
				(AVG(t2.AVG_of_Detrend_sm_multi)) AS AVG_of_AVG_of_Detrend_sm_multi, 
			  /* AVG_of_AVG_of_Detrend_multi */
				(AVG(t2.AVG_of_Detrend_multi)) AS AVG_of_AVG_of_Detrend_multi
		  FROM casuser.pbo_dictionary_ml t1
			   INNER JOIN casuser.QUERY_FOR_TSDSTIMESERIESOUT_001D t2 ON (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID)
		  GROUP BY t2.CHANNEL_CD,
				   t1.BUILDING_TYPE,
				   t2.WOY,
				   t2.DOW;
	QUIT;

	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.REGION_SEASONALITY_WBY_0000{options replace=true} AS 
	   SELECT t2.CHANNEL_CD, 
			  t1.BUILDING_TYPE, 
			  t2.WBY, 
			  t2.DOW, 
			  /* AVG_of_AVG_of_Detrend_sm_multi */
				(AVG(t2.AVG_of_Detrend_sm_multi)) AS AVG_of_AVG_of_Detrend_sm_multi, 
			  /* AVG_of_AVG_of_Detrend_multi */
				(AVG(t2.AVG_of_Detrend_multi)) AS AVG_of_AVG_of_Detrend_multi
		  FROM casuser.pbo_dictionary_ml t1
			   INNER JOIN casuser.QUERY_FOR_TSDSTIMESERIESOUT_001D t2 ON (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID)
		  GROUP BY t2.CHANNEL_CD,
				   t1.BUILDING_TYPE,
				   t2.WBY,
				   t2.DOW;
	QUIT;

	/* 21.  */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.REGION_REST_SEAS_WBY{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t2.CHANNEL_CD, 
			  t2.Region, 
			  t2.BUILDING_TYPE, 
			  t2.WBY, 
			  t2.DOW, 
			  t2.AVG_of_AVG_of_Detrend_sm_multi, 
			  t2.AVG_of_AVG_of_Detrend_multi
		  FROM casuser.pbo_dictionary_ml t1, casuser.REGION_SEASONALITY_WBY t2
		  WHERE (t1.BUILDING_TYPE = t2.BUILDING_TYPE AND t1.LVL2_ID = t2.Region);
	QUIT;

	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.REGION_REST_SEAS_0001{options replace=true} AS 
	   SELECT t2.CHANNEL_CD, 
	/*           t2.Region,  */
			  t2.BUILDING_TYPE, 
			  t1.PBO_LOCATION_ID, 
			  t2.WOY, 
			  t2.DOW, 
			  t2.AVG_of_AVG_of_Detrend_sm_multi, 
			  t2.AVG_of_AVG_of_Detrend_multi
		  FROM casuser.pbo_dictionary_ml t1, casuser.REGION_SEASONALITY_0001 t2
		  WHERE (t1.BUILDING_TYPE = t2.BUILDING_TYPE)
	;
	QUIT;

	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.REGION_REST_SEAS_WBY_0000{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t2.CHANNEL_CD, 
	/*           t2.Region,  */
			  t2.BUILDING_TYPE, 
			  t2.WBY, 
			  t2.DOW, 
			  t2.AVG_of_AVG_of_Detrend_sm_multi, 
			  t2.AVG_of_AVG_of_Detrend_multi
		  FROM casuser.pbo_dictionary_ml t1, casuser.REGION_SEASONALITY_WBY_0000 t2
		  WHERE (t1.BUILDING_TYPE = t2.BUILDING_TYPE);
	QUIT;

	/* 22.  */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.REGION_REST_SEAS{options replace=true} AS 
	   SELECT t2.CHANNEL_CD, 
			  t2.Region, 
			  t2.BUILDING_TYPE,
			  t1.PBO_LOCATION_ID, 
			  t2.WOY, 
			  t2.DOW, 
			  t2.AVG_of_AVG_of_Detrend_sm_multi, 
			  t2.AVG_of_AVG_of_Detrend_multi 
		  FROM casuser.pbo_dictionary_ml t1, casuser.REGION_SEASONALITY t2
		  WHERE (t1.BUILDING_TYPE = t2.BUILDING_TYPE AND t1.LVL2_ID = t2.Region)
	;
	QUIT;

	/* 23.  */
	FILENAME REFFILE DISK '/data/files/input/mcd_covid_pattern_day.csv';

	PROC IMPORT DATAFILE=REFFILE
		DBMS=CSV
		OUT=WORK.MCD_COVID_PATTERN_DAY;
		GETNAMES=YES;
	RUN;
	
	proc casutil;
	  load data=WORK.MCD_COVID_PATTERN_DAY casout='MCD_COVID_PATTERN_DAY' outcaslib='casuser' replace;
	run;
	
	data CASUSER.PBO_LOCATION (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..pbo_location(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
	
	data CASUSER.PBO_LOC_HIERARCHY (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..PBO_LOC_HIERARCHY(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	data CASUSER.PBO_LOC_ATTRIBUTES (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..PBO_LOC_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	/* Создаем таблицу связывающую PBO на листовом уровне и на любом другом */
	proc fedsql sessref=casauto;
		create table casuser.pbo_hier_flat{options replace=true} as
			select
				t1.pbo_location_id, 
				t2.PBO_LOCATION_ID as LVL3_ID,
				t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
				1 as LVL1_ID
			from 
				(select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=4) as t1
			left join 
				(select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=3) as t2
			on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
		;
		create table casuser.lvl4{options replace=true} as 
			select distinct
				pbo_location_id as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_hier_flat
		;
		create table casuser.lvl3{options replace=true} as 
			select distinct
				LVL3_ID as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_hier_flat
		;
		create table casuser.lvl2{options replace=true} as 
			select distinct
				LVL2_ID as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_hier_flat
		;
		create table casuser.lvl1{options replace=true} as 
			select 
				1 as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				casuser.pbo_hier_flat
		;
	quit;

	/* Соединяем в единый справочник ПБО */
	data casuser.pbo_lvl_all;
		set casuser.lvl4 casuser.lvl3 casuser.lvl2 casuser.lvl1;
	run;


	/* Добавляем к таблице промо ПБО и товары */
	proc fedsql sessref = casauto;
		create table casuser.promo_x_pbo_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t2.PBO_LEAF_ID
			from
				casuser.promo_pbo_enh as t1,
				casuser.pbo_lvl_all as t2
			where t1.pbo_location_id = t2.PBO_LOCATION_ID
		;
	quit;

	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.PROMO_GROUP_PBO{options replace=true} AS 
	   SELECT DISTINCT
	/* 		  t1.CHANNEL_CD,  */
			  t2.PBO_LEAF_ID as PBO_LOCATION_ID, 
			  t1.PROMO_GROUP_ID,
			  datepart(t1.START_DT) as START_DT,
			  datepart(t1.END_DT) as END_DT,
			  weekday(datepart(t1.start_dt))
		  FROM casuser.promo_enh t1
		  INNER JOIN casuser.promo_x_pbo_leaf t2
		  ON t1.PROMO_ID = t2.PROMO_ID
	;
	QUIT;

	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.TRP{options replace=true} AS 
	   SELECT t1.PROMO_GROUP_ID, 
			  t1.REPORT_DT AS REPORT_DT, 
			  t1.TRP, 
			  DATEPART(t1.REPORT_DT) AS REPORT_WK
		  FROM casuser.MEDIA_ENH t1
	;
	QUIT;

	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.TRP_PBO{options replace=true} AS 
	   SELECT t1.PROMO_GROUP_ID, 
			  t1.REPORT_DT, 
			  t1.TRP, 
			  t1.REPORT_WK, 
			  t2.PROMO_GROUP_ID AS PROMO_GROUP_ID1, 
			  t2.PBO_LOCATION_ID, 
	/*           t2.CHANNEL_CD,  */
			  t2.START_DT, 
			  t2.END_DT
		  FROM casuser.TRP t1
		  LEFT JOIN casuser.PROMO_GROUP_PBO t2 
		  ON (t1.PROMO_GROUP_ID = t2.PROMO_GROUP_ID) 
			AND (t1.REPORT_DT >= t2.START_DT) 
			AND (t1.REPORT_DT <= t2.END_DT)
	;
	QUIT;


	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.TRP_PBO_SUM{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t1.REPORT_WK, 
			  t1.REPORT_DT, 
			  SUM(t1.TRP) AS SUM_TRP
		  FROM casuser.TRP_PBO t1
		  GROUP BY t1.PBO_LOCATION_ID,
				   t1.REPORT_WK,
				   t1.REPORT_DT
	;
	QUIT;

	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.GC_TRAIN_ABT_TRP{options replace=true} AS 
	   SELECT t1.PBO_LOCATION_ID, 
			  t1.CHANNEL_CD, 
			  t1.new_RECEIPT_QTY, 
			  t1.RECEIPT_QTY, 
			  t1.SALES_DT, 
			  t1.WOY, 
			  t1.WBY, 
			  t1.DOW, 
			  (LOG(t7.SUM_TRP)) AS SUM_TRP_LOG,
			  /* COVID_pattern */
				(COALESCE(t2.COVID_pattern, 0)) AS COVID_pattern, 
			  /* COVID_lockdown */
				(CASE  
				   WHEN t2.COVID_pattern ^= .
				   THEN 1
				   ELSE 0
				END) AS COVID_lockdown, 
			  /* COVID_level */
				(CASE  
				   WHEN t1.SALES_DT >= date '2020-03-16'
				   THEN 1
				   ELSE 0
				END) AS COVID_level, 

			  /* AVG_of_Detrend_sm_multi */
				(COALESCE(t1.AVG_of_Detrend_sm_multi, t3.AVG_of_AVG_of_Detrend_sm_multi, t5.AVG_of_AVG_of_Detrend_sm_multi)) AS AVG_of_Detrend_sm_multi, 
			  /* AVG_of_Detrend_multi */
				(COALESCE(t1.AVG_of_Detrend_multi, t3.AVG_of_AVG_of_Detrend_multi, t5.AVG_of_AVG_of_Detrend_multi)) AS AVG_of_Detrend_multi, 
			  /* AVG_of_Detrend_sm_multi_WBY */
				(COALESCE(t1.AVG_of_Detrend_sm_multi_WBY, t4.AVG_of_AVG_of_Detrend_sm_multi, t6.AVG_of_AVG_of_Detrend_sm_multi)) AS AVG_of_Detrend_sm_multi_WBY, 
			  /* AVG_of_Detrend_multi_WBY */
				(COALESCE(t1.AVG_of_Detrend_multi_WBY, t4.AVG_of_AVG_of_Detrend_multi, t6.AVG_of_AVG_of_Detrend_multi)) AS AVG_of_Detrend_multi_WBY, 

			  /* Detrend_sm_multi */
				(CASE  
				   WHEN MONTH(t1.SALES_DT) = 12
				   THEN (COALESCE(t1.AVG_of_Detrend_sm_multi_WBY, t4.AVG_of_AVG_of_Detrend_sm_multi, t6.AVG_of_AVG_of_Detrend_sm_multi))
				   ELSE (COALESCE(t1.AVG_of_Detrend_sm_multi, t3.AVG_of_AVG_of_Detrend_sm_multi, t5.AVG_of_AVG_of_Detrend_sm_multi))
				END) AS Detrend_sm_multi, 
			  /* Detrend_multi */
				(CASE  
				   WHEN MONTH(t1.SALES_DT) = 12
				   THEN (COALESCE(t1.AVG_of_Detrend_multi_WBY, t4.AVG_of_AVG_of_Detrend_multi, t6.AVG_of_AVG_of_Detrend_multi))
				   ELSE (COALESCE(t1.AVG_of_Detrend_multi, t3.AVG_of_AVG_of_Detrend_multi, t5.AVG_of_AVG_of_Detrend_multi))
				END) AS Detrend_multi, 
			  /* Deseason_multi */
				(t1.RECEIPT_QTY / (CASE  
				   WHEN MONTH(t1.SALES_DT) = 12
				   THEN (COALESCE(t1.AVG_of_Detrend_multi_WBY, t4.AVG_of_AVG_of_Detrend_multi, t6.AVG_of_AVG_of_Detrend_multi))
				   ELSE (COALESCE(t1.AVG_of_Detrend_multi, t3.AVG_of_AVG_of_Detrend_multi, t5.AVG_of_AVG_of_Detrend_multi))
				END)) AS Deseason_multi, 
			  /* Deseason_sm_multi */
				(t1.new_RECEIPT_QTY / (CASE  
				   WHEN MONTH(t1.SALES_DT) = 12
				   THEN (COALESCE(t1.AVG_of_Detrend_sm_multi_WBY, t4.AVG_of_AVG_of_Detrend_sm_multi, t6.AVG_of_AVG_of_Detrend_sm_multi))
				   ELSE (COALESCE(t1.AVG_of_Detrend_sm_multi, t3.AVG_of_AVG_of_Detrend_sm_multi, t5.AVG_of_AVG_of_Detrend_sm_multi))
				END)) AS Deseason_sm_multi
		  FROM casuser.QUERY_FOR_TSDSTIMESERIESOUT_000F t1
			   LEFT JOIN casuser.MCD_COVID_PATTERN_DAY t2 ON (t1.CHANNEL_CD = t2.CHANNEL_CD) AND (t1.SALES_DT = t2.SALES_DT)
			   LEFT JOIN casuser.REGION_REST_SEAS t3 ON (t1.CHANNEL_CD = t3.CHANNEL_CD) AND (t1.PBO_LOCATION_ID = 
			  t3.PBO_LOCATION_ID) AND (t1.WOY = t3.WOY) AND (t1.DOW = t3.DOW)
			   LEFT JOIN casuser.REGION_REST_SEAS_WBY t4 ON (t1.PBO_LOCATION_ID = t4.PBO_LOCATION_ID) AND (t1.CHANNEL_CD = 
			  t4.CHANNEL_CD) AND (t1.WBY = t4.WBY) AND (t1.DOW = t4.DOW)
			   LEFT JOIN casuser.REGION_REST_SEAS_0001 t5 ON (t1.PBO_LOCATION_ID = t5.PBO_LOCATION_ID) AND (t1.CHANNEL_CD = 
			  t5.CHANNEL_CD) AND (t1.WBY = t5.WOY) AND (t1.DOW = t5.DOW)
			   LEFT JOIN casuser.REGION_REST_SEAS_WBY_0000 t6 ON (t1.PBO_LOCATION_ID = t6.PBO_LOCATION_ID) AND (t1.CHANNEL_CD = 
			  t6.CHANNEL_CD) AND (t1.WBY = t6.WBY) AND (t1.DOW = t6.DOW)
			   LEFT JOIN casuser.TRP_PBO_SUM t7 ON (t1.PBO_LOCATION_ID = t7.PBO_LOCATION_ID) AND (t1.SALES_WK 
			  = t7.REPORT_WK)
	;
	QUIT;

	proc casutil;
		droptable incaslib='casuser' casdata='PBO_SM_TRAIN_TRP' quiet;
	run;

	/* 25.  */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.PBO_SM_TRAIN_TRP{options replace=true} AS 
	   SELECT t1.CHANNEL_CD, 
			  t1.PBO_LOCATION_ID, 
			  t1.SALES_DT, 
			  t1.COVID_pattern, 
			  t1.COVID_lockdown, 
			  t1.COVID_level, 
			  t1.SUM_TRP_LOG,
			  /* Target */
				(CASE  
				   WHEN t1.SALES_DT >= &fcst_start_dt.
				   THEN .
				   ELSE t1.Deseason_sm_multi
				END) AS Target
		  FROM casuser.GC_TRAIN_ABT_TRP t1
		  WHERE t1.CHANNEL_CD = 'ALL';
	QUIT;
	
	proc casutil;
		droptable incaslib="&lmvLibrefOut." casdata="&lmvTabNmOut." quiet;
		droptable incaslib="&lmvLibrefOutABT." casdata="&lmvTabNmOutABT." quiet;
	run;
		
	DATA &lmvLibrefOut..&lmvTabNmOut.(promote=yes);
		set casuser.PBO_SM_TRAIN_TRP(where=(sales_dt>=intnx('year', sales_dt, -4, 'B')));
		format sales_dt date9.;
	RUN;
	
	DATA &lmvLibrefOutABT..&lmvTabNmOutABT.(promote=yes);
		set casuser.GC_TRAIN_ABT_TRP(where=(sales_dt>=intnx('year', sales_dt, -4, 'B')));
		format sales_dt date9.;
	RUN;
	
	proc casutil;
		save incaslib="&lmvLibrefOut." outcaslib="&lmvLibrefOut." casdata="&lmvTabNmOut." casout="&lmvTabNmOut..sashdat" replace; 
		save incaslib="&lmvLibrefOutABT." outcaslib="&lmvLibrefOutABT." casdata="&lmvTabNmOutABT." casout="&lmvTabNmOutABT..sashdat" replace; 
	run;
	
%mend fcst_create_abt_pbo_gc;