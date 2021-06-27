/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для запуска VF-проекта на основе pbo_sal_abt
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
*     %vf004_run_project_pbo;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf004_run_project_pbo;

	/* Получение токена аутентификации */
	%tech_get_token(mpUsername=&SYS_ADM_USER., mpOutToken=tmp_token);
	
	%tech_update_resource_status(mpStatus=P, mpResource=vf_prepare_ts_abt_pbo);
	/*Запуск job в SASJobExecution */
	filename resp TEMP;
	proc http
			method="GET"
			url="https://10.252.151.9/SASJobExecution/?_program=/Maintenance_jobs/vf004_run_project_pbo"
			out=resp;
			headers
				"Authorization"="bearer &tmp_token."
				"Accept"="application/vnd.sas.job.execution.job+json";
	run;

	/* Ресурсы обновляются в джобе в JobExecution Maintenance_jobs/vf004_run_project_pbo */
	*%tech_update_resource_status(mpStatus=L, mpResource=vf_prepare_ts_abt_pbo);
	
	*%tech_open_resource(mpResource=vf_run_project_pbo);
									
%mend vf004_run_project_pbo;