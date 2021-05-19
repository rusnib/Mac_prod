%macro fcst_restore_seasonality(mpInputTbl= MN_DICT.TRAIN_ABT_TRP
							 ,mpMode=PBO
							 ,mpOutTableNm = mn_dict.pbo_forecast_restored
							 ,mpAuth = YES
							 );

	%tech_cas_session(mpMode = start
							,mpCasSessNm = casauto
							,mpAssignFlg= y
							);

	%let forecast_start_dt = date '2020-10-01';
	%let forecast_end_dt = date '2020-10-31';

	%local	lmvMode
			lmvInputTbl
			lmvProjectId
			lmvLibrefOut
			lmvTabNmOut
			lmvVfPmixName
	;
	%let lmvInputTbl = &mpInputTbl.;
	%let lmvMode=&mpMode.;
	%member_names (mpTable=&mpOutTableNm, mpLibrefNameKey=lmvLibrefOut, mpMemberNameKey=lmvTabNmOut);
	
	%if &mpAuth. = YES %then %do;
		%tech_get_token(mpUsername=ru-nborzunov, mpOutToken=tmp_token);
		
		filename resp TEMP;
		proc http
		  method="GET"
		  url="&CUR_API_URL./analyticsGateway/projects?limit=99999"
		  out=resp;
		  headers 
			"Authorization"="bearer &tmp_token."
			"Accept"="application/vnd.sas.collection+json";    
		run;
		%put Response status: &SYS_PROCHTTP_STATUS_CODE;
		
		libname respjson JSON fileref=resp;
		
		data work.vf_project_list;
		  set respjson.items;
		run;
	%end;
	%else %if &mpAuth. = NO %then %do;
		%vf_get_project_list(mpOut=work.vf_project_list);
	%end;
	
	/* Извлечение ID для VF-проекта по его имени */
	%let lmvVfPmixName = &&VF_&lmvMode._NM.;
	%let lmvProjectId = %vf_get_project_id_by_name(mpName=&lmvVfPmixName., mpProjList=work.vf_project_list);

	/* Drop target table */
	proc casutil;
		droptable casdata="&lmvTabNmOut." incaslib="&lmvLibrefOut." quiet;
	run;
	
	/* 1. Get forecast horizon from project */
	PROC FEDSQL sessref=casauto;
	   CREATE TABLE casuser.HORIZON{options replace=true} AS 
	   SELECT 
			t1.CHANNEL_CD,
			t1.PBO_LOCATION_ID,
			t1.SALES_DT,
			t1.PREDICT as PREDICT_SM
	   FROM "Analytics_Project_&lmvProjectId.".horizon t1
	;
	QUIT;

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
			  t1.AVG_of_Detrend_sm_multi, 
			 /* t1.AVG_of_Detrend_sm_aggreg, */
			  t1.AVG_of_Detrend_multi, 
			 /* t1.AVG_of_Detrend_aggreg,  */
			  t1.AVG_of_Detrend_sm_multi_WBY, 
			  t1.AVG_of_Detrend_multi_WBY, 
			/*  t1.AVG_of_Detrend_sm_aggreg_WBY, */
			/*  t1.AVG_of_Detrend_aggreg_WBY, */
			  t1.Detrend_sm_multi, 
			  t1.Detrend_multi, 
			  t1.Deseason_sm_multi, 
			  t1.Deseason_multi, 
			  t1.COVID_pattern, 
			  t1.COVID_lockdown, 
			  t1.COVID_level, 
			  (t3.PREDICT_SM * t1.Detrend_multi) AS &lmvMode._FCST
		  FROM &lmvInputTbl. t1 /* входной параметр */
			   LEFT JOIN casuser.HORIZON t3 ON (t1.CHANNEL_CD = t3.CHANNEL_CD) AND 
			  (t1.PBO_LOCATION_ID = t3.PBO_LOCATION_ID) AND (t1.SALES_DT = t3.SALES_DT)
		  WHERE t1.SALES_DT between &forecast_start_dt. and &forecast_end_dt.
	;
	QUIT;

			
	proc casutil;
		promote casdata='forecast_restored' incaslib='casuser' outcaslib="&lmvLibrefOut." casout="&lmvTabNmOut.";
		save incaslib="&lmvLibrefOut." outcaslib="&lmvLibrefOut." casdata="&lmvTabNmOut." casout="&lmvTabNmOut..sashdat" replace; 
	run;
	
%mend fcst_restore_seasonality;