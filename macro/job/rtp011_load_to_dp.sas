/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки csv в DP
*	
*
*  ПАРАМЕТРЫ:
*     Нет
*
******************************************************************
*  Использует: 
*	  нет
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %rtp011_load_to_dp;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro rtp011_load_to_dp;

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_log_event(mpMode=START, mpProcess_Nm=rtp_load_to_dp);				
	%tech_update_resource_status(mpStatus=P, mpResource=rtp_komp_sep);
	
		%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FoM_KOMP);

		%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoM_KOMP);
		%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoD_KOMP);


		%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FoM_KOMP);
		%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FoD_KOMP);


		%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FoM_NONKOMP);
		%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FoD_NONKOMP);

		%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoM_NONKOMP);
		%dp_jobexecution(mpJobName=ACT_LOAD_GC_FoD_NONKOMP);

		%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FoM_NONKOMP);
		%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FoD_NONKOMP);
		
		%macro load_csv_to_dp(mpJobName=ACT_LOAD_QNT_FoD_KOMP);
			filename resp TEMP;
			%let lmvJobName=&mpJobName.;
			%let lmvUrl=&CUR_API_URL.;
			%global SYS_PROCHTTP_STATUS_CODE SYS_PROCHTTP_STATUS_PHRASE;
			%let SYS_PROCHTTP_STATUS_CODE=;
			%let SYS_PROCHTTP_STATUS_PHRASE=;
			filename jsn temp;
			proc http
				url="&lmvUrl./retailAnalytics/processModels/"
				method="GET"
				out=jsn 
				OAUTH_BEARER=SAS_SERVICES;
			run;

			libname posts JSON fileref=jsn ;
			title "Automap of JSON data";

			proc datasets noprint;
			   copy in= posts out=work memtype=data;
			   run; 
			quit;

			proc sql noprint;
				create table process_template as 
				select a.*,b.href,b.uri
				from ITEMS as a left join ITEMS_EXECUTE as b
				on a.ordinal_items=b.ordinal_items
				;
			quit;


			proc sql noprint;
				select %str(href) into: lmvJobUrl
				from process_template
				where name="&lmvJobName";
			quit;

			%put &=lmvJobUrl;


			proc http
				url="&lmvUrl.&lmvJobUrl."
				method="POST"
				OAUTH_BEARER=SAS_SERVICES
				out=resp;
				headers
				"Content-Type" ="application/vnd.sas.retail.process.data+json";
			run;
			
			%let SERVICESBASEURL=10.252.151.3/;

			libname respjson JSON fileref=resp;
			%put &=SYS_PROCHTTP_STATUS_CODE &=SYS_PROCHTTP_STATUS_PHRASE;
			%echo_File(resp);


			%local stateUri;
			%let stateUri=;
			  data _null_;
				set respjson.links;
				if rel='state' then 
					call symput('stateUri', uri);
			  run;
			
			%local jobState;
			
			%do %until(&jobState ^= running);
			
			  proc http
				method="GET"
				url="&SERVICESBASEURL.&stateUri"
				out=resp
				OAUTH_BEARER=SAS_SERVICES;
			  run;
			  %put Response status: &SYS_PROCHTTP_STATUS_CODE;
			
			  %echo_File(resp);
			  libname respjs1 JSON fileref=resp;
			  data _null_;
				 set respjs1.root;
				call symputx('jobState', state);
			  run;
			
			  %put jobState = &jobState;	
			
			  data _null_;
				call sleep(50000);
			  run;
			
			%end;
			
			%if not (&jobState = completed) %then %do;
			  %put ERROR: An invalid response was received.;
			  %abort;
			%end;
			
		%mend load_csv_to_dp;
		%load_csv_to_dp(mpJobName=ACT_LOAD_QNT_FoD_KOMP);
		
		/* start seeding */
		%dp_jobexecution(mpJobName=ACT_SEED_COMP_SALE_MONTH);
		%dp_jobexecution(mpJobName=ACT_SEED_COMP_SALE_DAY);
		%dp_jobexecution(mpJobName=ACT_SEED_COMP_GC_MONTH);
		%dp_jobexecution(mpJobName=ACT_SEED_COMP_GC_DAY);
		%dp_jobexecution(mpJobName=ACT_SEED_COMP_UPT_MONTH);
		%dp_jobexecution(mpJobName=ACT_SEED_COMP_UPT_DAY);
		
		%dp_jobexecution(mpJobName=ACT_QNT_SEED_MON_NONKOMP);
		%dp_jobexecution(mpJobName=ACT_QNT_SEED_DAY_NONKOMP);
		%dp_jobexecution(mpJobName=ACT_GC_SEED_MON_NONKOMP);
		%dp_jobexecution(mpJobName=ACT_GC_SEED_DAY_NONKOMP);
		%dp_jobexecution(mpJobName=ACT_UPT_SEED_MON_NONKOMP);
		%dp_jobexecution(mpJobName=ACT_UPT_SEED_DAY_NONKOMP);

	%tech_update_resource_status(mpStatus=L, mpResource=rtp_komp_sep);
	%tech_open_resource(mpResource=rtp_load_to_dp);
	
	%tech_log_event(mpMode=END, mpProcess_Nm=rtp_load_to_dp);	
	
%mend rtp011_load_to_dp;