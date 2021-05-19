/* %fcst_restore_seasonality( */
/* 		   mpInputTbl	= PUBLIC.NIK_T2 */
/* 		 , mpMode		= GC */
/* 		 , mpOutTableNm = CASUSER.gc_forecast_restored */
/* 		 , mpAuth 		= NO */
/* 	 ); */


/******************************************************************************/
/* Compare holdout=3mth vs. houldout=0 */
PROC FEDSQL sessref=casauto;
   CREATE TABLE casuser.GC_FINAL_COMPARE_HOLDOUTS{options replace=true} AS 
   SELECT t1.PBO_LOCATION_ID, 
          t1.CHANNEL_CD, 
		t1.sales_dt,
          t1.Forecast_daily_sm7 as Forecast_daily_sm7_h0,
		  t1.Forecast_daily_week as Forecast_daily_week_h0,
		  t3.Forecast_daily_sm7 as Forecast_daily_sm7_h3,
		  t3.Forecast_daily_week as Forecast_daily_week_h3
      FROM CASUSER.gc_forecast t1
           inner JOIN CASUSER.gc_forecast_holdout t3 ON (t1.CHANNEL_CD = t3.CHANNEL_CD) AND 
          (t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID) AND (t1.SALES_DT = t3.SALES_DT)
;
QUIT;

proc casutil;
	promote casdata='GC_FINAL_COMPARE_HOLDOUTS' incaslib='casuser' outcaslib='casuser' casout='GC_FINAL_COMPARE_HOLDOUTS';
run;

/******************************************************************************/
/* Compare holdout=3mth vs. houldout=0 */
PROC FEDSQL sessref=casauto;
   CREATE TABLE casuser.GC_FINAL_COMPARE_HOLDOUTS2{options replace=true} AS 
   SELECT t1.PBO_LOCATION_ID, 
          t1.CHANNEL_CD, 
		t1.sales_dt,
          t1.Forecast_daily_sm7 as Forecast_daily_sm7_h0,
		  t1.Forecast_daily_week as Forecast_daily_week_h0,
		  t3.Forecast_daily_sm7 as Forecast_daily_sm7_h12,
		  t3.Forecast_daily_week as Forecast_daily_week_h12
      FROM CASUSER.gc_forecast t1
           inner JOIN CASUSER.gc_forecast_holdout12 t3 ON (t1.CHANNEL_CD = t3.CHANNEL_CD) AND 
          (t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID) AND (t1.SALES_DT = t3.SALES_DT)
;
QUIT;

proc casutil;
	promote casdata='GC_FINAL_COMPARE_HOLDOUTS2' incaslib='casuser' outcaslib='casuser' casout='GC_FINAL_COMPARE_HOLDOUTS2';
run;


/******************************************************************************/
/* Compare PROD's script with ANNA's script */
PROC FEDSQL sessref=casauto;
   CREATE TABLE casuser.GC_FINAL_COMPARE{options replace=true} AS 
   SELECT t1.PBO_LOCATION_ID, 
          t1.CHANNEL_CD, 
		t1.sales_dt,
          t1.Forecast_daily_sm7,
		  t1.Forecast_daily_week as GC_FCST_ANNA,
		  t1.PREDICT_SM,
          t3.GC_FCST as GC_FCST_PROD
      FROM CASUSER.gc_forecast t1
           LEFT JOIN CASUSER.gc_forecast_restored t3 ON (t1.CHANNEL_CD = t3.CHANNEL_CD) AND 
          (t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID) AND (t1.SALES_DT = t3.SALES_DT)
;
QUIT;

proc casutil;
	promote casdata='GC_FINAL_COMPARE' incaslib='casuser' outcaslib='casuser' casout='GC_FINAL_COMPARE';
run;




/******************************************************************************/
/* Compare with Anna's Forecast from 1may2021 */
/* Import */
FILENAME REFFILE DISK '/home/ru-mpovod/my_data/MAY_JUNE_JULY_FCST_ANNA_RAW.xlsx';

PROC IMPORT DATAFILE=REFFILE
	DBMS=XLSX
	OUT=WORK.IMPORT;
	GETNAMES=YES;
RUN;

data CASUSER.FORECAST_SENT_1MAY;
set WORK.IMPORT;
format new_sales_dt date9.;
new_sales_dt = input(sales_dt, date9.);
Forecast_daily_sm7_upd = input(Forecast_daily_sm7, best32.8);
Forecast_daily_week_upd = input(Forecast_daily_week, best32.8);
run; 

PROC FEDSQL sessref=casauto;
   CREATE TABLE casuser.test_1may{options replace=true} AS 
   SELECT count(distinct PBO_LOCATION_ID) as count_loc
		  ,sales_dt
      FROM CASUSER.FORECAST_SENT_1MAY t1
          group by sales_dt
;
  CREATE TABLE casuser.dist_pbo_1may{options replace=true} AS 
   SELECT distinct PBO_LOCATION_ID
      FROM CASUSER.FORECAST_SENT_1MAY
;
QUIT;

data CASUSER.FORECAST_FROM_11MAY;
set CASUSER.GC_FORECAST;
WHERE CHANNEL_CD = 'ALL';
format new_sales_dt date9.;
new_sales_dt = sales_dt;
run; 

PROC FEDSQL sessref=casauto;
   CREATE TABLE casuser.test_11may{options replace=true} AS 
   SELECT count(distinct PBO_LOCATION_ID) as count_loc
		  ,sales_dt
      FROM CASUSER.FORECAST_FROM_11MAY t1
          group by sales_dt
;
QUIT;

PROC FEDSQL sessref=casauto;
   CREATE TABLE casuser.GC_COMPARE_FCSTS{options replace=true} AS 
   SELECT 
		   coalesce(t1.PBO_LOCATION_ID , t3.PBO_LOCATION_ID ) as PBO_LOCATION_ID 
		  ,coalesce(t1.new_sales_dt , t3.new_sales_dt ) as SALES_DT 
          ,t1.Forecast_daily_sm7 as Forecast_daily_sm7_11may
		  ,t3.Forecast_daily_sm7_upd as Forecast_daily_sm7_01may
		  ,t1.Forecast_daily_week as GC_FCST_11may
		  ,t3.Forecast_daily_week_upd as GC_FCST_01may
      FROM CASUSER.FORECAST_FROM_11MAY t1
           full JOIN casuser.FORECAST_SENT_1MAY t3 
	ON (t1.CHANNEL_CD = t3.CHANNEL_CD) 
		AND (t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID) 
		AND (t1.new_sales_dt = t3.new_sales_dt)
;
QUIT;


proc casutil;
	promote casdata='GC_COMPARE_FCSTS' incaslib='casuser' outcaslib='casuser' casout='GC_COMPARE_FCSTS';
run;

PROC FEDSQL sessref=casauto;
   CREATE TABLE casuser._GC_FINAL_COMPARE_1{options replace=true} AS 
   SELECT 
		t1.sales_dt,
          sum(t1.GC_FCST_01may) as GC_FCST_01may,
		  sum(t1.GC_FCST_11may) as GC_FCST_11may
      FROM CASUSER.GC_COMPARE_FCSTS_v2 t1
		group by t1.sales_dt
;
 CREATE TABLE casuser._GC_FINAL_COMPARE_2{options replace=true} AS 
   SELECT 
		avg(GC_FCST_01may) as avg_GC_FCST_01may,
		avg(GC_FCST_11may) as avg_GC_FCST_11may
      FROM CASUSER._GC_FINAL_COMPARE_1 
;
QUIT;

PROC FEDSQL sessref=casauto;
CREATE TABLE casuser.GC_COMPARE_FCSTS_v2{options replace=true} AS 
   SELECT t1.*
from  casuser.GC_COMPARE_FCSTS as t1
inner join casuser.dist_pbo_1may t3
on t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID
;
QUIT;


proc casutil;
	promote casdata='GC_COMPARE_FCSTS_v2' incaslib='casuser' outcaslib='casuser' casout='GC_COMPARE_FCSTS_v2';
run;

libname MAX_LIB "/home/ru-mpovod/my_data";
data MAX_LIB.GC_FORECAST_FROM_11MAY;
set CASUSER.FORECAST_FROM_11MAY;
run;




data MAX_LIB.GC_FORECAST_HOLDOUT0;
set CASUSER.GC_FORECAST;
run;

data MAX_LIB.GC_FORECAST_HOLDOUT3;
set CASUSER.GC_FORECAST_HOLDOUT;
run;

data MAX_LIB.GC_FORECAST_HOLDOUT12;
set CASUSER.GC_FORECAST_HOLDOUT12;
run;
