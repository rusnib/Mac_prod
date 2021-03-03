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

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	/* Получение токена аутентификации */
	%tech_get_token(mpUsername=ru-nborzunov, mpOutToken=tmp_token);
	
	%tech_update_resource_status(mpStatus=P, mpResource=vf_prepare_ts_abt_pbo);
	
	%vf_run_project_rec(mpProjectName=&VF_PBO_PROJ_NM.); 

	%tech_update_resource_status(mpStatus=L, mpResource=vf_prepare_ts_abt_pbo);
	
	%tech_open_resource(mpResource=vf_run_project_pbo);
	
	%tech_cas_session(mpMode = end
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
%mend vf004_run_project_pbo;