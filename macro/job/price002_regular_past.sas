/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки регулярных цен на прошлое в CAS в рамках 
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
*     %price002_regular_past;
*
****************************************************************************
*  03-03-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro price002_regular_past;

	%tech_log_event(mpMode=START, mpProcess_Nm=price_regular_past);

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=price_load_data);
	
	%price_regular_past(mpPromoTable    	    = CASUSER.PROMO
						, mpPromoPboTable       = CASUSER.PROMO_PBO_UNFOLD
						, mpPromoProdTable      = CASUSER.PROMO_PROD
						, mpProductAttrTable    = CASUSER.PRODUCT_ATTRIBUTES
						, mpPboLocAttributes	= CASUSER.PBO_LOC_ATTRIBUTES
						, mpPriceTable 		    = CASUSER.PRICE
						, mpVatTable 		    = CASUSER.VAT
						, mpPriceIncreaseTable 	= CASUSER.PRICE_INCREASE
						, mpOutTable 		    = MN_DICT.PRICE_REGULAR_PAST
						, mpBatchValue 		    = 50
						);

	%tech_update_resource_status(mpStatus=L, mpResource=price_load_data);
	
	%tech_open_resource(mpResource=price_regular_past);

	%tech_log_event(mpMode=END, mpProcess_Nm=price_regular_past);

%mend price002_regular_past;