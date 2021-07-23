/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*      
*	 
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
*     %;
*
****************************************************************************
*  
****************************************************************************/
%macro price006_load_price_unf;

	%tech_log_event(mpMode=START, mpProcess_Nm=load_price_unfold);

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=price_promo_future);
	
	%price_unfold;
	
	%tech_update_resource_status(mpStatus=L, mpResource=price_promo_future);
	
	%tech_open_resource(mpResource=load_price_unfold);

	%tech_log_event(mpMode=END, mpProcess_Nm=load_price_unfold);

%mend price006_load_price_unf;