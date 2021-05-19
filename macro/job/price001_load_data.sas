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
*     %price001_load_data;
*
****************************************************************************
*  03-03-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro price001_load_data;

	%tech_log_event(mpMode=START, mpProcess_Nm=price_load_data);	

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=price);
	
	/* 1. загрузка данных в CAS */
	%price_load_data;

	%tech_update_resource_status(mpStatus=L, mpResource=price);
	
	%tech_open_resource(mpResource=price_load_data);

	%tech_log_event(mpMode=END, mpProcess_Nm=price_load_data);	

%mend price001_load_data;