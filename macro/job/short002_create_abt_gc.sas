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
*     %short002_create_abt_gc;
*
****************************************************************************
*  03-03-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro short002_create_abt_gc;

	%tech_log_event(mpMode=START, mpProcess_Nm=short_create_abt_gc);	

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=short_create_abt_pbo);
	
	%fcst_create_abt_pbo_gc(mpMode=gc
							 ,mpOutTableDmVf = MN_DICT.DM_TRAIN_TRP_GC
							 ,mpOutTableDmABT = MN_DICT.TRAIN_ABT_TRP_GC
							 );

	%tech_update_resource_status(mpStatus=L, mpResource=short_create_abt_pbo);
	
	%tech_open_resource(mpResource=short_create_abt_gc);

	%tech_log_event(mpMode=END, mpProcess_Nm=short_create_abt_gc);	

%mend short002_create_abt_gc;