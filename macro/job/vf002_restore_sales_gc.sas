/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для восстановления GC
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
*     %vf002_restore_sales_gc;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf002_restore_sales_gc;
	
	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
	
	%tech_log_event(mpMode=START, mpProcess_Nm=vf_restore_sales_gc);	
						
	%tech_update_resource_status(mpStatus=P, mpResource=vf_load_data);
	
	%vf_restore_sales_gc;

	%tech_update_resource_status(mpStatus=L, mpResource=vf_load_data);
	
	%tech_open_resource(mpResource=vf_restore_sales_gc);
	
	%tech_log_event(mpMode=END, mpProcess_Nm=vf_restore_sales_gc);	
	
%mend vf002_restore_sales_gc;