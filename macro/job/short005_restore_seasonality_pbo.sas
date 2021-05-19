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
*     %short005_restore_seasonality_pbo;
*
****************************************************************************
*  03-03-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro short005_restore_seasonality_pbo;

	%tech_log_event(mpMode=START, mpProcess_Nm=short_restore_seasonality_pbo);	

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=short_vf_run_project_gc);
	%tech_get_token(mpUsername=ru-nborzunov, mpOutToken=tmp_token);
	%fcst_restore_seasonality(mpInputTbl= MN_DICT.TRAIN_ABT_TRP_PBO
							 ,mpMode=PBO
							 ,mpOutTableNm = mn_dict.pbo_forecast_restored
							 ,mpAuth = YES
							 );

	%tech_update_resource_status(mpStatus=L, mpResource=short_vf_run_project_gc);
	
	%tech_open_resource(mpResource=short_restore_seasonality_pbo);

	%tech_log_event(mpMode=END, mpProcess_Nm=short_restore_seasonality_pbo);	

%mend short005_restore_seasonality_pbo;