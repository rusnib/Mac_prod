/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки промо цен на прошлое в CAS в рамках 
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
*     %price004_promo_past;
*
****************************************************************************
*  03-03-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro price004_promo_past;

	%tech_log_event(mpMode=START, mpProcess_Nm=price_promo_past);

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=price_regular_future);
	
	%price_promo_past(mpPriceRegPastTab    = MN_DICT.PRICE_REGULAR_PAST
						, mpPromoTable       = CASUSER.PROMO
						, mpPromoPboTable    = CASUSER.PROMO_PBO_UNFOLD
						, mpPromoProdTable   = CASUSER.PROMO_PROD
						, mpProductAttrTable = CASUSER.PRODUCT_ATTRIBUTES
						, mpVatTable 		 = CASUSER.VAT
						, mpOutTable 		 = MN_DICT.PRICE_PROMO_PAST
						, mpBatchValue 		 = 50
						);
	%tech_update_resource_status(mpStatus=L, mpResource=price_regular_future);
	
	%tech_open_resource(mpResource=price_promo_past);

	%tech_log_event(mpMode=END, mpProcess_Nm=price_promo_past);

%mend price004_promo_past;