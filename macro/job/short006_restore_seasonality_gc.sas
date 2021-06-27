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
*     %short006_restore_seasonality_gc;
*
****************************************************************************
*  03-03-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro short006_restore_seasonality_gc;

	%tech_log_event(mpMode=START, mpProcess_Nm=short_restore_seasonality_gc);	

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=short_restore_seasonality_pbo);
	%tech_get_token(mpUsername=&SYS_ADM_USER., mpOutToken=tmp_token);
	%fcst_restore_seasonality(mpInputTbl= MN_DICT.TRAIN_ABT_TRP_GC
							 ,mpMode=GC
							 ,mpOutTableNm = mn_dict.gc_forecast_restored
							 ,mpAuth = YES
							 );

	%tech_update_resource_status(mpStatus=L, mpResource=short_restore_seasonality_pbo);
	
	%tech_open_resource(mpResource=short_restore_seasonality_gc);

	%tech_log_event(mpMode=END, mpProcess_Nm=short_restore_seasonality_gc);	

%mend short006_restore_seasonality_gc;