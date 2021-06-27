
%macro assign;
%let casauto_ok = %sysfunc(SESSFOUND ( casauto)) ;
%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
 cas casauto;
 caslib _all_ assign;
%end;
%mend;
%assign

/* LIBNAME ETL_STG "/data2/etl_stg_23_11_2020"; */
/* %let inlib=ETL_STG; */

%let forecast_start_dt = date '2021-05-19';
%let forecast_end_dt = date '2021-08-19';


%let project_id =3d54d821-4c3f-41cb-9232-75bc920f8f48;


/* 1. Get forecast horizon from project */
PROC FEDSQL sessref=casauto;
   CREATE TABLE casuser.HORIZON_SM{options replace=true} AS 
   SELECT 
        t1.CHANNEL_CD,
		t1.PBO_LOCATION_ID,
		t1.SALES_DT,
		t1.PREDICT as PREDICT_SM
   FROM "Analytics_Project_&project_id.".horizon t1
;
QUIT;
/*  */

proc casutil incaslib='casuser';
	droptable casdata='gc_forecast_holdout' quiet;
run;

/* 2. Restore seasonality */
PROC FEDSQL sessref=casauto;
   CREATE TABLE casuser.FORECAST_RESTORED{options replace=true} AS 
   SELECT t1.PBO_LOCATION_ID, 
          t1.CHANNEL_CD, 
          t1.new_RECEIPT_QTY, 
          t1.RECEIPT_QTY, 
          t1.SALES_DT, 
          t1.WOY, 
          t1.WBY, 
          t1.DOW, 
          t1.COVID_pattern, 
          t1.COVID_lockdown, 
          t1.COVID_level, 
          t3.PREDICT_SM, 
/*           Forecast_daily_sm7 */
            (t3.PREDICT_SM * t1.Detrend_sm_multi) AS Forecast_daily_sm7, 
/*           Forecast_daily_week */
            (t3.PREDICT_SM * t1.Detrend_multi) AS Forecast_daily_week
      FROM /*PUBLIC.GC_TRAIN_ABT*/ CASUSER.GC_TRAIN_ABT_TRP t1
           LEFT JOIN casuser.HORIZON_SM t3 ON (t1.CHANNEL_CD = t3.CHANNEL_CD) AND 
          (t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID) AND (t1.SALES_DT = t3.SALES_DT)
   	  WHERE t1.SALES_DT between &forecast_start_dt. and &forecast_end_dt.
;
QUIT;



proc casutil;
	promote casdata='forecast_restored' incaslib='casuser' outcaslib='max_casl' casout='gc_forecast_may19_ho12';
run;