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
*     %short001_create_abt_pbo;
*
****************************************************************************
*  03-03-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro short001_create_abt_pbo;

	%tech_log_event(mpMode=START, mpProcess_Nm=short_create_abt_pbo);	

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=pmix_sales_rtp);
	
	%fcst_create_abt_pbo_gc(mpMode=pbo
							 ,mpOutTableDmVf = MN_DICT.DM_TRAIN_TRP_PBO
							 ,mpOutTableDmABT = MN_DICT.TRAIN_ABT_TRP_PBO
							 );

	%tech_update_resource_status(mpStatus=L, mpResource=pmix_sales_rtp);
	
	%tech_open_resource(mpResource=short_create_abt_pbo);

	%tech_log_event(mpMode=END, mpProcess_Nm=short_create_abt_pbo);	

%mend short001_create_abt_pbo;