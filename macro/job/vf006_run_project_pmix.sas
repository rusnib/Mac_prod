/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для запуска VF-проекта на основе pmix_sal_abt
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
*     %vf006_run_project_pmix;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf006_run_project_pmix;

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	/* Получение токена аутентификации */
	%tech_get_token(mpUsername=&SYS_ADM_USER., mpOutToken=tmp_token);
	
	%tech_update_resource_status(mpStatus=P, mpResource=vf_prepare_ts_abt_pmix);
	
	*%vf_run_project_rec(mpProjectName=&VF_PMIX_PROJ_NM.); 
	filename resp TEMP;
	proc http
			method="GET"
			url="https://10.252.151.9/SASJobExecution/?_program=/Maintenance_jobs/vf006_run_project_pmix"
			/* oauth_bearer = sas_services */
			out=resp;
			headers
				"Authorization"="bearer &tmp_token."
				"Accept"="application/vnd.sas.job.execution.job+json";
		run;
	
	/* Ресурсы обновляются в JobExecution /Maintenance_jobs/vf006_run_project_pmix */
	*%tech_update_resource_status(mpStatus=L, mpResource=vf_prepare_ts_abt_pmix);
	
	*%tech_open_resource(mpResource=vf_run_project_pmix);
	*%tech_open_resource(mpResource=rtp_abt_pmix_prepare_after_vf);
	
%mend vf006_run_project_pmix;