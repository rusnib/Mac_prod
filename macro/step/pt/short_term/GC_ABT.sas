%macro assign;
%let casauto_ok = %sysfunc(SESSFOUND ( casauto)) ;
%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
 cas casauto;
 caslib _all_ assign;
%end;
%mend;
%assign

/* LIBNAME ETL_STG "/data2/etl_stg_23_11_2020"; */
%let inlib=IA;
%let sas_history_end_dt = '10may2021'd;
%let history_end_dt = date '2021-05-10';

%let start_date = '11may2021'd;
%let end_date = '10aug2021'd;
%let fcst_start_dt = date '2021-05-11';



/* 1. Timeseries MA[7] */
/* proc casutil; */
/*   load data=&inlib..ia_pbo_sales_history casout='ia_pbo_sales_history' outcaslib='public' replace; */
/* run; */

PROC SORT
	DATA=WORK.PBO_SALES /*IA.ia_pbo_sales_history*/ (KEEP=SALES_DT RECEIPT_QTY PBO_LOCATION_ID CHANNEL_CD)
	OUT=WORK.TMP0TempTableInput
	;
	BY PBO_LOCATION_ID CHANNEL_CD SALES_DT;
RUN;

PROC EXPAND DATA=WORK.TMP0TempTableInput
	OUT=PUBLIC.MA7_TIMESERIES
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
/* proc casutil; */
/* 	load data=ETL_STG.comp_geo_for_nov2020 casout='comp_geo_for_nov2020' outcaslib='Public' replace; */
/* run; */

PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.MA7_TIMESERIES_CMP{options replace=true} AS 
   SELECT t1.PBO_LOCATION_ID, 
          t1.CHANNEL_CD, 
          t1.new_RECEIPT_QTY, 
          t1.RECEIPT_QTY, 
          /* SALES_DT */
            (DATEPART(t1.SALES_DT)) AS SALES_DT
      FROM PUBLIC.MA7_TIMESERIES t1
/*            INNER JOIN PUBLIC.comp_geo_for_nov2020 t3  */
/* 	  ON (t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID) */
	  WHERE DATEPART(t1.SALES_DT) <= &history_end_dt.
;
QUIT;


data public.dates;
	do SALES_DT=&start_date. to &end_date.;
	new_RECEIPT_QTY = .;
	RECEIPT_QTY = .;
	output;
	end;
	format SALES_DT DDMMYYP.;
run;

PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.MA7_CMP_DISTINCT{options replace=true} AS 
   SELECT DISTINCT t1.CHANNEL_CD, 
          t1.PBO_LOCATION_ID
      FROM PUBLIC.MA7_TIMESERIES_CMP t1
;
QUIT;

/* 7. */
PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.FUTURE_SKELETON{options replace=true} AS 
   SELECT t1.CHANNEL_CD, 
          t1.PBO_LOCATION_ID, 
          t2.SALES_DT, 
          t2.new_RECEIPT_QTY, 
          t2.RECEIPT_QTY
      FROM PUBLIC.MA7_CMP_DISTINCT t1
           CROSS JOIN PUBLIC.DATES t2;
QUIT;

/* 8. Append future to history */
data public.SALES_FULL;
	set public.MA7_TIMESERIES_CMP public.FUTURE_SKELETON;
run;

/* 9. Add WOY and DOW and other stuff */
PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.SALES_WITH_WOY_DOW{options replace=true} AS 
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
      FROM PUBLIC.SALES_FULL t1
;
QUIT;

/* 10.  */
PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.PRE_COVID_WOY_DOW{options replace=true} AS 
   SELECT t1.PBO_LOCATION_ID, 
          t1.CHANNEL_CD, 
          t1.new_RECEIPT_QTY AS new_RECEIPT_QTY_weekly, 
          t1.RECEIPT_QTY, 
          t1.SALES_DT, 
          t1.WOY, 
          t1.DOW, 
          t1.WBY
      FROM PUBLIC.SALES_WITH_WOY_DOW t1
      WHERE t1.SALES_DT < date '2020-03-01';
QUIT;

/* 11.  */
PROC SORT
	DATA=PUBLIC.PRE_COVID_WOY_DOW(KEEP=SALES_DT RECEIPT_QTY CHANNEL_CD PBO_LOCATION_ID)
	OUT=WORK.TMP0TempTableInput
	;
	BY CHANNEL_CD PBO_LOCATION_ID SALES_DT;
RUN;

PROC EXPAND DATA=WORK.TMP0TempTableInput
	OUT=PUBLIC.MA364_TIMESERIES(LABEL="Modified Time Series data for PUBLIC.PRE_COVID_WOY_DOW")
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
   CREATE TABLE PUBLIC.MA364_WITH_WOY_DOW{options replace=true} AS 
   SELECT t1.PBO_LOCATION_ID, 
          t1.CHANNEL_CD, 
          t1.new_RECEIPT_QTY_weekly, 
          t1.RECEIPT_QTY, 
          t1.SALES_DT, 
          t1.WOY, 
          t1.DOW, 
          t1.WBY, 
          t2.new_RECEIPT_QTY AS new_RECEIPT_QTY_yearly
      FROM PUBLIC.PRE_COVID_WOY_DOW t1, PUBLIC.MA364_TIMESERIES t2
      WHERE (t1.CHANNEL_CD = t2.CHANNEL_CD AND t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID AND t1.SALES_DT = t2.SALES_DT);
QUIT;

/* 13.  */
PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.MA364_DETREND_DESEASON{options replace=true} AS 
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
          /* Detrend_aggreg */
            (t1.RECEIPT_QTY - t1.new_RECEIPT_QTY_yearly) AS Detrend_aggreg, 
          /* Detrend_sm_multi */
            (t1.new_RECEIPT_QTY_weekly / t1.new_RECEIPT_QTY_yearly) AS Detrend_sm_multi, 
          /* Detrend_sm_aggreg */
            (t1.new_RECEIPT_QTY_weekly -  t1.new_RECEIPT_QTY_yearly) AS Detrend_sm_aggreg
      FROM PUBLIC.MA364_WITH_WOY_DOW t1
      WHERE t1.SALES_DT >= date '2017-07-05' AND t1.SALES_DT <= date '2019-09-15';
QUIT;

/* 14.  */
PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.QUERY_FOR_TSDSTIMESERIESOUT_000D{options replace=true} AS 
   SELECT t1.CHANNEL_CD, 
          t1.PBO_LOCATION_ID, 
          t1.WOY, 
          t1.DOW, 
          /* AVG_of_Detrend_sm_multi */
            (AVG(t1.Detrend_sm_multi)) AS AVG_of_Detrend_sm_multi, 
          /* AVG_of_Detrend_sm_aggreg */
            (AVG(t1.Detrend_sm_aggreg)) AS AVG_of_Detrend_sm_aggreg, 
          /* AVG_of_Detrend_multi */
            (AVG(t1.Detrend_multi)) AS AVG_of_Detrend_multi, 
          /* AVG_of_Detrend_aggreg */
            (AVG(t1.Detrend_aggreg)) AS AVG_of_Detrend_aggreg
      FROM PUBLIC.MA364_DETREND_DESEASON t1
      GROUP BY t1.CHANNEL_CD,
               t1.PBO_LOCATION_ID,
               t1.WOY,
               t1.DOW;
QUIT;

/* 15.  */
PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.QUERY_FOR_TSDSTIMESERIESOUT_0018{options replace=true} AS 
   SELECT t1.CHANNEL_CD, 
          t1.PBO_LOCATION_ID, 
          t1.WBY, 
          t1.DOW, 
          /* AVG_of_Detrend_sm_multi */
            (AVG(t1.Detrend_sm_multi)) AS AVG_of_Detrend_sm_multi, 
          /* AVG_of_Detrend_sm_aggreg */
            (AVG(t1.Detrend_sm_aggreg)) AS AVG_of_Detrend_sm_aggreg, 
          /* AVG_of_Detrend_multi */
            (AVG(t1.Detrend_multi)) AS AVG_of_Detrend_multi, 
          /* AVG_of_Detrend_aggreg */
            (AVG(t1.Detrend_aggreg)) AS AVG_of_Detrend_aggreg
      FROM PUBLIC.MA364_DETREND_DESEASON t1
      GROUP BY t1.CHANNEL_CD,
               t1.PBO_LOCATION_ID,
               t1.WBY,
               t1.DOW;
QUIT;

/* 16.  */
PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.QUERY_FOR_TSDSTIMESERIESOUT_000F{options replace=true} AS 
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
          t2.AVG_of_Detrend_sm_aggreg, 
          t2.AVG_of_Detrend_multi, 
          t2.AVG_of_Detrend_aggreg, 
          t3.AVG_of_Detrend_sm_multi AS AVG_of_Detrend_sm_multi_WBY, 
          t3.AVG_of_Detrend_sm_aggreg AS AVG_of_Detrend_sm_aggreg_WBY, 
          t3.AVG_of_Detrend_multi AS AVG_of_Detrend_multi_WBY, 
          t3.AVG_of_Detrend_aggreg AS AVG_of_Detrend_aggreg_WBY
      FROM PUBLIC.SALES_WITH_WOY_DOW t1
           LEFT JOIN PUBLIC.QUERY_FOR_TSDSTIMESERIESOUT_000D t2 ON (t1.CHANNEL_CD = t2.CHANNEL_CD) AND 
          (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID) AND (t1.WOY = t2.WOY) AND (t1.DOW = t2.DOW)
           LEFT JOIN PUBLIC.QUERY_FOR_TSDSTIMESERIESOUT_0018 t3 ON (t1.CHANNEL_CD = t3.CHANNEL_CD) AND 
          (t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID) AND (t1.DOW = t3.DOW) AND (t1.WBY = t3.WBY)
/*       ORDER BY t1.CHANNEL_CD, */
/*                t1.PBO_LOCATION_ID, */
/*                t1.SALES_DT */
;
QUIT;

/* 17.  */
PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.QUERY_FOR_TSDSTIMESERIESOUT_001D{options replace=true} AS 
   SELECT t1.CHANNEL_CD, 
          t1.SALES_DT, 
          t1.PBO_LOCATION_ID, 
          t1.WOY, 
          t1.DOW, 
          t1.WBY, 
          t1.AVG_of_Detrend_sm_multi, 
          t1.AVG_of_Detrend_sm_aggreg, 
          t1.AVG_of_Detrend_multi, 
          t1.AVG_of_Detrend_aggreg
      FROM PUBLIC.QUERY_FOR_TSDSTIMESERIESOUT_000F t1
      WHERE t1.AVG_of_Detrend_sm_multi ^= .;
QUIT;

/* 18.  PBO ATTRIBUTES */
proc casutil;
  load data=&inlib..IA_pbo_location casout='ia_pbo_location' outcaslib='public' replace;
  load data=&inlib..IA_PBO_LOC_HIERARCHY casout='IA_PBO_LOC_HIERARCHY' outcaslib='public' replace;
  load data=&inlib..IA_PBO_LOC_ATTRIBUTES casout='IA_PBO_LOC_ATTRIBUTES' outcaslib='public' replace;
run;

proc cas;
transpose.transpose /
   table={name="ia_pbo_loc_attributes", caslib="public", groupby={"pbo_location_id"}} 
   attributes={{name="pbo_location_id"}} 
   transpose={"PBO_LOC_ATTR_VALUE"} 
   prefix="" 
   id={"PBO_LOC_ATTR_NM"} 
   casout={name="attr_transposed", caslib="public", replace=true};
quit;

proc fedsql sessref=casauto;
   create table public.pbo_hier_flat{options replace=true} as
		select t1.pbo_location_id, 
			   t2.PBO_LOCATION_ID as LVL3_ID,
			   t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
			   1 as LVL1_ID
		from 
		(select * from public.ia_pbo_loc_hierarchy where pbo_location_lvl=4) as t1
		left join 
		(select * from public.ia_pbo_loc_hierarchy where pbo_location_lvl=3) as t2
		on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
 		;
quit;

proc fedsql sessref=casauto;
	create table public.pbo_dictionary_ml{options replace=true} as
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
			public.pbo_hier_flat t2
		left join
			public.attr_transposed t3
		on
			t2.pbo_location_id=t3.pbo_location_id
		left join
			PUBLIC.IA_PBO_LOCATION t14
		on 
			t2.pbo_location_id=t14.pbo_location_id
		left join
			PUBLIC.IA_PBO_LOCATION t13
		on 
			t2.lvl3_id=t13.pbo_location_id
		left join
			PUBLIC.IA_PBO_LOCATION t12
		on
			t2.lvl2_id=t12.pbo_location_id;
quit;

/* 19.  */
PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.REGION_SEASONALITY_WBY{options replace=true} AS 
   SELECT t2.CHANNEL_CD, 
          t1.LVL2_ID AS Region, 
          t1.BUILDING_TYPE, 
          t2.WBY, 
          t2.DOW, 
          /* AVG_of_AVG_of_Detrend_sm_multi */
            (AVG(t2.AVG_of_Detrend_sm_multi)) AS AVG_of_AVG_of_Detrend_sm_multi, 
          /* AVG_of_AVG_of_Detrend_sm_aggreg */
            (AVG(t2.AVG_of_Detrend_sm_aggreg)) AS AVG_of_AVG_of_Detrend_sm_aggreg, 
          /* AVG_of_AVG_of_Detrend_multi */
            (AVG(t2.AVG_of_Detrend_multi)) AS AVG_of_AVG_of_Detrend_multi, 
          /* AVG_of_AVG_of_Detrend_aggreg */
            (AVG(t2.AVG_of_Detrend_aggreg)) AS AVG_of_AVG_of_Detrend_aggreg
      FROM PUBLIC.pbo_dictionary_ml t1
           INNER JOIN PUBLIC.QUERY_FOR_TSDSTIMESERIESOUT_001D t2 ON (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID)
      GROUP BY t2.CHANNEL_CD,
               t1.LVL2_ID,
               t1.BUILDING_TYPE,
               t2.WBY,
               t2.DOW;
QUIT;

/* 20.  */
PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.REGION_SEASONALITY{options replace=true} AS 
   SELECT t2.CHANNEL_CD, 
          t1.LVL2_ID AS Region, 
          t1.BUILDING_TYPE, 
          t2.WOY, 
          t2.DOW, 
          /* AVG_of_AVG_of_Detrend_sm_multi */
            (AVG(t2.AVG_of_Detrend_sm_multi)) AS AVG_of_AVG_of_Detrend_sm_multi, 
          /* AVG_of_AVG_of_Detrend_sm_aggreg */
            (AVG(t2.AVG_of_Detrend_sm_aggreg)) AS AVG_of_AVG_of_Detrend_sm_aggreg, 
          /* AVG_of_AVG_of_Detrend_multi */
            (AVG(t2.AVG_of_Detrend_multi)) AS AVG_of_AVG_of_Detrend_multi, 
          /* AVG_of_AVG_of_Detrend_aggreg */
            (AVG(t2.AVG_of_Detrend_aggreg)) AS AVG_of_AVG_of_Detrend_aggreg
      FROM PUBLIC.pbo_dictionary_ml t1
           INNER JOIN PUBLIC.QUERY_FOR_TSDSTIMESERIESOUT_001D t2 ON (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID)
      GROUP BY t2.CHANNEL_CD,
               t1.LVL2_ID,
               t1.BUILDING_TYPE,
               t2.WOY,
               t2.DOW;
QUIT;

PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.REGION_SEASONALITY_0001{options replace=true} AS 
   SELECT t2.CHANNEL_CD, 
          t1.BUILDING_TYPE, 
          t2.WOY, 
          t2.DOW, 
          /* AVG_of_AVG_of_Detrend_sm_multi */
            (AVG(t2.AVG_of_Detrend_sm_multi)) AS AVG_of_AVG_of_Detrend_sm_multi, 
          /* AVG_of_AVG_of_Detrend_sm_aggreg */
            (AVG(t2.AVG_of_Detrend_sm_aggreg)) AS AVG_of_AVG_of_Detrend_sm_aggreg, 
          /* AVG_of_AVG_of_Detrend_multi */
            (AVG(t2.AVG_of_Detrend_multi)) AS AVG_of_AVG_of_Detrend_multi, 
          /* AVG_of_AVG_of_Detrend_aggreg */
            (AVG(t2.AVG_of_Detrend_aggreg)) AS AVG_of_AVG_of_Detrend_aggreg
      FROM PUBLIC.pbo_dictionary_ml t1
           INNER JOIN PUBLIC.QUERY_FOR_TSDSTIMESERIESOUT_001D t2 ON (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID)
      GROUP BY t2.CHANNEL_CD,
               t1.BUILDING_TYPE,
               t2.WOY,
               t2.DOW;
QUIT;

PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.REGION_SEASONALITY_WBY_0000{options replace=true} AS 
   SELECT t2.CHANNEL_CD, 
          t1.BUILDING_TYPE, 
          t2.WBY, 
          t2.DOW, 
          /* AVG_of_AVG_of_Detrend_sm_multi */
            (AVG(t2.AVG_of_Detrend_sm_multi)) AS AVG_of_AVG_of_Detrend_sm_multi, 
          /* AVG_of_AVG_of_Detrend_sm_aggreg */
            (AVG(t2.AVG_of_Detrend_sm_aggreg)) AS AVG_of_AVG_of_Detrend_sm_aggreg, 
          /* AVG_of_AVG_of_Detrend_multi */
            (AVG(t2.AVG_of_Detrend_multi)) AS AVG_of_AVG_of_Detrend_multi, 
          /* AVG_of_AVG_of_Detrend_aggreg */
            (AVG(t2.AVG_of_Detrend_aggreg)) AS AVG_of_AVG_of_Detrend_aggreg
      FROM PUBLIC.pbo_dictionary_ml t1
           INNER JOIN PUBLIC.QUERY_FOR_TSDSTIMESERIESOUT_001D t2 ON (t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID)
      GROUP BY t2.CHANNEL_CD,
               t1.BUILDING_TYPE,
               t2.WBY,
               t2.DOW;
QUIT;

/* 21.  */
PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.REGION_REST_SEAS_WBY{options replace=true} AS 
   SELECT t1.PBO_LOCATION_ID, 
          t2.CHANNEL_CD, 
          t2.Region, 
          t2.BUILDING_TYPE, 
          t2.WBY, 
          t2.DOW, 
          t2.AVG_of_AVG_of_Detrend_sm_multi, 
          t2.AVG_of_AVG_of_Detrend_sm_aggreg, 
          t2.AVG_of_AVG_of_Detrend_multi, 
          t2.AVG_of_AVG_of_Detrend_aggreg
      FROM PUBLIC.pbo_dictionary_ml t1, PUBLIC.REGION_SEASONALITY_WBY t2
      WHERE (t1.BUILDING_TYPE = t2.BUILDING_TYPE AND t1.LVL2_ID = t2.Region);
QUIT;

PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.REGION_REST_SEAS_0001{options replace=true} AS 
   SELECT t2.CHANNEL_CD, 
/*           t2.Region,  */
          t2.BUILDING_TYPE, 
          t1.PBO_LOCATION_ID, 
          t2.WOY, 
          t2.DOW, 
          t2.AVG_of_AVG_of_Detrend_sm_multi, 
          t2.AVG_of_AVG_of_Detrend_sm_aggreg, 
          t2.AVG_of_AVG_of_Detrend_multi, 
          t2.AVG_of_AVG_of_Detrend_aggreg
      FROM PUBLIC.pbo_dictionary_ml t1, PUBLIC.REGION_SEASONALITY_0001 t2
      WHERE (t1.BUILDING_TYPE = t2.BUILDING_TYPE)
/*       ORDER BY t2.CHANNEL_CD, */
/*                t2.Region, */
/*                t2.BUILDING_TYPE, */
/*                t1.PBO_LOCATION_ID, */
/*                t2.WOY, */
/*                t2.DOW */
;
QUIT;

PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.REGION_REST_SEAS_WBY_0000{options replace=true} AS 
   SELECT t1.PBO_LOCATION_ID, 
          t2.CHANNEL_CD, 
/*           t2.Region,  */
          t2.BUILDING_TYPE, 
          t2.WBY, 
          t2.DOW, 
          t2.AVG_of_AVG_of_Detrend_sm_multi, 
          t2.AVG_of_AVG_of_Detrend_sm_aggreg, 
          t2.AVG_of_AVG_of_Detrend_multi, 
          t2.AVG_of_AVG_of_Detrend_aggreg
      FROM PUBLIC.pbo_dictionary_ml t1, PUBLIC.REGION_SEASONALITY_WBY_0000 t2
      WHERE (t1.BUILDING_TYPE = t2.BUILDING_TYPE);
QUIT;

/* 22.  */
PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.REGION_REST_SEAS{options replace=true} AS 
   SELECT t2.CHANNEL_CD, 
          t2.Region, 
          t2.BUILDING_TYPE,
          t1.PBO_LOCATION_ID, 
          t2.WOY, 
          t2.DOW, 
          t2.AVG_of_AVG_of_Detrend_sm_multi, 
          t2.AVG_of_AVG_of_Detrend_sm_aggreg, 
          t2.AVG_of_AVG_of_Detrend_multi, 
          t2.AVG_of_AVG_of_Detrend_aggreg
      FROM PUBLIC.pbo_dictionary_ml t1, PUBLIC.REGION_SEASONALITY t2
      WHERE (t1.BUILDING_TYPE = t2.BUILDING_TYPE AND t1.LVL2_ID = t2.Region)
	;
QUIT;

/* 23.  */
/* proc casutil; */
/*   load data=&inlib..MCD_COVID_PATTERN_DAY casout='MCD_COVID_PATTERN_DAY' outcaslib='public' replace; */
/* run; */

	FILENAME REFFILE DISK '/data/files/input/mcd_covid_pattern_day.csv';

	PROC IMPORT DATAFILE=REFFILE
		DBMS=CSV
		OUT=WORK.MCD_COVID_PATTERN_DAY;
		GETNAMES=YES;
	RUN;
	
	proc casutil;
	  load data=WORK.MCD_COVID_PATTERN_DAY casout='MCD_COVID_PATTERN_DAY' outcaslib='public' replace;
	run;



/*************** Добавление TRP *****************/


/* proc casutil outcaslib='Public'; */
/* 	load data=&inlib..media_enh casout='ia_media' replace; */
/* 	load data=&inlib..promo_enh casout='ia_promo' replace; */
/* 	load data=&inlib..promo_pbo_enh casout='ia_promo_x_pbo' replace;	 */
/* 	load data=&inlib..ia_pbo_loc_hierarchy casout='ia_pbo_loc_hierarchy' replace; */
/* 	load data=&inlib..ia_product_hierarchy casout='ia_product_hierarchy' replace; */
/* run; */

	%add_promotool_marks2(mpOutCaslib=public,
							mpPtCaslib=pt);

/* Создаем таблицу связывающую PBO на листовом уровне и на любом другом */
proc fedsql sessref=casauto;
	create table public.pbo_hier_flat{options replace=true} as
		select
			t1.pbo_location_id, 
			t2.PBO_LOCATION_ID as LVL3_ID,
			t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
			1 as LVL1_ID
		from 
			(select * from public.ia_pbo_loc_hierarchy where pbo_location_lvl=4) as t1
		left join 
			(select * from public.ia_pbo_loc_hierarchy where pbo_location_lvl=3) as t2
		on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
	;
	create table public.lvl4{options replace=true} as 
		select distinct
			pbo_location_id as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			public.pbo_hier_flat
	;
	create table public.lvl3{options replace=true} as 
		select distinct
			LVL3_ID as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			public.pbo_hier_flat
	;
	create table public.lvl2{options replace=true} as 
		select distinct
			LVL2_ID as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			public.pbo_hier_flat
	;
	create table public.lvl1{options replace=true} as 
		select 
			1 as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			public.pbo_hier_flat
	;
quit;

/* Соединяем в единый справочник ПБО */
data public.pbo_lvl_all;
	set public.lvl4 public.lvl3 public.lvl2 public.lvl1;
run;


/* Добавляем к таблице промо ПБО и товары */
proc fedsql sessref = casauto;
	create table public.ia_promo_x_pbo_leaf{options replace = true} as 
		select distinct
			t1.promo_id,
			t2.PBO_LEAF_ID
		from
			public.promo_pbo_enh as t1,
			public.pbo_lvl_all as t2
		where t1.pbo_location_id = t2.PBO_LOCATION_ID
	;
quit;

PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.PROMO_GROUP_PBO{options replace=true} AS 
   SELECT DISTINCT
/* 		  t1.CHANNEL_CD,  */
          t2.PBO_LEAF_ID as PBO_LOCATION_ID, 
          t1.PROMO_GROUP_ID,
		  datepart(t1.START_DT) as START_DT,
		  datepart(t1.END_DT) as END_DT,
		  weekday(datepart(t1.start_dt))
      FROM public.PROMO_enh t1
	  INNER JOIN PUBLIC.IA_PROMO_X_PBO_LEAF t2
	  ON t1.PROMO_ID = t2.PROMO_ID
;
QUIT;

PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.TRP{options replace=true} AS 
   SELECT t1.PROMO_GROUP_ID, 
          t1.REPORT_DT AS REPORT_DT, 
          t1.TRP, 
		  DATEPART(t1.REPORT_DT) AS REPORT_WK
      FROM public.MEDIA_enh t1
;
QUIT;

PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.TRP_PBO{options replace=true} AS 
   SELECT t1.PROMO_GROUP_ID, 
          t1.REPORT_DT, 
          t1.TRP, 
          t1.REPORT_WK, 
          t2.PROMO_GROUP_ID AS PROMO_GROUP_ID1, 
          t2.PBO_LOCATION_ID, 
/*           t2.CHANNEL_CD,  */
          t2.START_DT, 
          t2.END_DT
      FROM PUBLIC.TRP t1
      LEFT JOIN PUBLIC.PROMO_GROUP_PBO t2 
	  ON (t1.PROMO_GROUP_ID = t2.PROMO_GROUP_ID) 
		AND (t1.REPORT_DT >= t2.START_DT) 
		AND (t1.REPORT_DT <= t2.END_DT)
/* 	  WHERE t2.CHANNEL_CD = 'ALL' */
;
QUIT;


PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.TRP_PBO_SUM{options replace=true} AS 
   SELECT t1.PBO_LOCATION_ID, 
          t1.REPORT_WK, 
          t1.REPORT_DT, 
          SUM(t1.TRP) AS SUM_TRP
      FROM PUBLIC.TRP_PBO t1
      GROUP BY t1.PBO_LOCATION_ID,
               t1.REPORT_WK,
               t1.REPORT_DT
;
QUIT;


/**
  * Создание полной ABT для GC
  *
  * WBY : Week Before Year, неделя до кончания года, вводится для того, чтобы учесть
  *       разное количество недель в разные года (возможно от 52 до 54 недель в году).
  *           В расчете сезонность считается на уровне [номер недели года, номер дня недели].
  *       В этом подходе существуют нюансы, одним из которых являются последние недели года.
  *       В разные года последняя неделя года может быть под номерами 52, 53, 54.
  *       Чтобы учесть этот нюанс в декабре сезонность считается по 
  *       [номер недели до конца года, номер дня недели] и стыкуется с обычной сезонность на
  *       стыке Ноябрь - Декабрь.
  *
  * SUM_TRP_LOG : суммарная рекламная поддержка всех промо. Для расчета GC агрегируется с уровня 
  *       товара до уровня ресторана по всем промо.
  *       Логарифм берется, т.к. после определенного уровня TRP явно видно насыщение спроса.
  *       Вместо логарифма можно попробовать использовать сигмоиду (логистическую функцию)
  *		  
  **/
  
PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.GC_TRAIN_ABT_TRP{options replace=true} AS 
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
          /* AVG_of_Detrend_sm_aggreg */
            (COALESCE(t1.AVG_of_Detrend_sm_aggreg, t3.AVG_of_AVG_of_Detrend_sm_aggreg, t5.AVG_of_AVG_of_Detrend_sm_aggreg)) AS AVG_of_Detrend_sm_aggreg, 
          /* AVG_of_Detrend_multi */
            (COALESCE(t1.AVG_of_Detrend_multi, t3.AVG_of_AVG_of_Detrend_multi, t5.AVG_of_AVG_of_Detrend_multi)) AS AVG_of_Detrend_multi, 
          /* AVG_of_Detrend_aggreg */
            (COALESCE(t1.AVG_of_Detrend_aggreg, t3.AVG_of_AVG_of_Detrend_aggreg, t5.AVG_of_AVG_of_Detrend_aggreg)) AS AVG_of_Detrend_aggreg, 
          /* AVG_of_Detrend_sm_multi_WBY */
            (COALESCE(t1.AVG_of_Detrend_sm_multi_WBY, t4.AVG_of_AVG_of_Detrend_sm_multi, t6.AVG_of_AVG_of_Detrend_sm_multi)) AS AVG_of_Detrend_sm_multi_WBY, 
          /* AVG_of_Detrend_multi_WBY */
            (COALESCE(t1.AVG_of_Detrend_multi_WBY, t4.AVG_of_AVG_of_Detrend_multi, t6.AVG_of_AVG_of_Detrend_multi)) AS AVG_of_Detrend_multi_WBY, 
          /* AVG_of_Detrend_sm_aggreg_WBY */
            (COALESCE(t1.AVG_of_Detrend_sm_aggreg_WBY, t4.AVG_of_AVG_of_Detrend_sm_aggreg, t6.AVG_of_AVG_of_Detrend_sm_aggreg)) AS 
            AVG_of_Detrend_sm_aggreg_WBY, 
          /* AVG_of_Detrend_aggreg_WBY */
            (COALESCE(t1.AVG_of_Detrend_aggreg_WBY, t4.AVG_of_AVG_of_Detrend_aggreg, t6.AVG_of_AVG_of_Detrend_aggreg)) AS AVG_of_Detrend_aggreg_WBY, 
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
      FROM PUBLIC.QUERY_FOR_TSDSTIMESERIESOUT_000F t1
           LEFT JOIN PUBLIC.MCD_COVID_PATTERN_DAY t2 ON (t1.CHANNEL_CD = t2.CHANNEL_CD) AND (t1.SALES_DT = t2.SALES_DT)
           LEFT JOIN PUBLIC.REGION_REST_SEAS t3 ON (t1.CHANNEL_CD = t3.CHANNEL_CD) AND (t1.PBO_LOCATION_ID = 
          t3.PBO_LOCATION_ID) AND (t1.WOY = t3.WOY) AND (t1.DOW = t3.DOW)
           LEFT JOIN PUBLIC.REGION_REST_SEAS_WBY t4 ON (t1.PBO_LOCATION_ID = t4.PBO_LOCATION_ID) AND (t1.CHANNEL_CD = 
          t4.CHANNEL_CD) AND (t1.WBY = t4.WBY) AND (t1.DOW = t4.DOW)
           LEFT JOIN PUBLIC.REGION_REST_SEAS_0001 t5 ON (t1.PBO_LOCATION_ID = t5.PBO_LOCATION_ID) AND (t1.CHANNEL_CD = 
          t5.CHANNEL_CD) AND (t1.WBY = t5.WOY) AND (t1.DOW = t5.DOW)
           LEFT JOIN PUBLIC.REGION_REST_SEAS_WBY_0000 t6 ON (t1.PBO_LOCATION_ID = t6.PBO_LOCATION_ID) AND (t1.CHANNEL_CD = 
          t6.CHANNEL_CD) AND (t1.WBY = t6.WBY) AND (t1.DOW = t6.DOW)
           LEFT JOIN PUBLIC.TRP_PBO_SUM t7 ON (t1.PBO_LOCATION_ID = t7.PBO_LOCATION_ID) AND (t1.SALES_WK 
          = t7.REPORT_WK)
;
QUIT;

proc casutil incaslib='Public' outcaslib='Public';
	droptable casdata='GC_SM_TRAIN_TRP' quiet;
run;

/* 25.  */
PROC FEDSQL sessref=casauto;
   CREATE TABLE PUBLIC.GC_SM_TRAIN_TRP{options replace=true} AS 
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
      FROM PUBLIC.GC_TRAIN_ABT_TRP t1
      WHERE t1.CHANNEL_CD = 'ALL'
/* 	  AND SALES_DT >= date '2018-01-01'  */
; 
QUIT;



proc casutil incaslib='Public' outcaslib='Public';
	promote casdata='GC_SM_TRAIN_TRP';
run;