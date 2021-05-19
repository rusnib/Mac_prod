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
*     %price005_promo_future;
*
****************************************************************************
*  03-03-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro price005_promo_future;

	%tech_log_event(mpMode=START, mpProcess_Nm=price_promo_future);

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=price_promo_past);
	
	%price_promo_future( mpPromoTable         	= CASUSER.PROMO
						, mpPromoPboTable 	 	= CASUSER.PROMO_PBO_UNFOLD
						, mpPromoProdTable   	= CASUSER.PROMO_PROD
						, mpPriceRegFutTable 	= MN_DICT.PRICE_REGULAR_FUTURE
						, mpVatTable		 	= CASUSER.VAT
						, mpLBPTable		 	= CASUSER.LBP
						, mpPboLocAttributes	= CASUSER.PBO_LOC_ATTRIBUTES
						, mpProductAttrTable    = CASUSER.PRODUCT_ATTRIBUTES
						, mpPriceIncreaseTable 	= CASUSER.PRICE_INCREASE
						, mpOutTable	  	 	= MN_DICT.PRICE_PROMO_FUTURE
						);

	%tech_update_resource_status(mpStatus=L, mpResource=price_promo_past);
	
	%tech_open_resource(mpResource=price_promo_future);

	%tech_log_event(mpMode=END, mpProcess_Nm=price_promo_future);

%mend price005_promo_future;