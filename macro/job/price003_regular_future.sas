/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки регулярных цен на будущее в CAS в рамках 
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
*     %price003_regular_future;
*
****************************************************************************
*  03-03-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro price003_regular_future;
	
	%tech_log_event(mpMode=START, mpProcess_Nm=price_regular_future);

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=price_regular_past);
	
	%price_regular_future(mpPriceRegTable   	    = CASUSER.VAT
							, mpProductAttrTable    = CASUSER.PRODUCT_ATTRIBUTES
							, mpPboLocAttributes	= CASUSER.PBO_LOC_ATTRIBUTES
							, mpPriceIncreaseTable 	= CASUSER.PRICE_INCREASE
							, mpOutTable 	  	    = MN_DICT.PRICE_REGULAR_FUTURE
							);

	%tech_update_resource_status(mpStatus=L, mpResource=price_regular_past);
	
	%tech_open_resource(mpResource=price_regular_future);
	
	%tech_log_event(mpMode=END, mpProcess_Nm=price_regular_future);

%mend price003_regular_future;