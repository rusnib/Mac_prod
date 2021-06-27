/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки данных в CAS в рамках 
*	  процесса подготовки цен
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
*     %short003_vf_run_project_pbo;
*
****************************************************************************
*  03-03-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro short003_vf_run_project_pbo;

	%tech_log_event(mpMode=START, mpProcess_Nm=short_vf_run_project_pbo);	

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=short_create_abt_gc);
	%tech_get_token(mpUsername=&SYS_ADM_USER., mpOutToken=tmp_token);
	%vf_run_project_rec(mpProjectName=&VF_PBO_NM.);

	%tech_update_resource_status(mpStatus=L, mpResource=short_create_abt_gc);
	
	%tech_open_resource(mpResource=short_vf_run_project_pbo);

	%tech_log_event(mpMode=END, mpProcess_Nm=short_vf_run_project_pbo);	

%mend short003_vf_run_project_pbo;