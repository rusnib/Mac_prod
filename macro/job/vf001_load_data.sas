/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки данных в CAS в рамках 
*	  сквозного процесса прогнозирования временными рядами
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
*     %vf001_load_data;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf001_load_data;
	
	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=pmix_sales);
	%tech_update_resource_status(mpStatus=P, mpResource=pbo_sales);
	%tech_update_resource_status(mpStatus=P, mpResource=product);
	%tech_update_resource_status(mpStatus=P, mpResource=cost_price);
	%tech_update_resource_status(mpStatus=P, mpResource=channel);
	%tech_update_resource_status(mpStatus=P, mpResource=comp_media);
	%tech_update_resource_status(mpStatus=P, mpResource=promo);
	%tech_update_resource_status(mpStatus=P, mpResource=promo_x_pbo);
	%tech_update_resource_status(mpStatus=P, mpResource=promo_x_product);
	
	/* 1. загрузка данных в CAS */
	%vf_load_data(mpEvents=mn_long.events
					,mpEventsMkup=mn_long.events_mkup
					,mpOutLibref = mn_long
					,mpClearFlg=YES);

	%tech_update_resource_status(mpStatus=L, mpResource=pmix_sales);
	%tech_update_resource_status(mpStatus=L, mpResource=pbo_sales);
	%tech_update_resource_status(mpStatus=L, mpResource=product);
	%tech_update_resource_status(mpStatus=L, mpResource=cost_price);
	%tech_update_resource_status(mpStatus=L, mpResource=channel);
	%tech_update_resource_status(mpStatus=L, mpResource=comp_media);
	%tech_update_resource_status(mpStatus=L, mpResource=promo);
	%tech_update_resource_status(mpStatus=L, mpResource=promo_x_pbo);
	%tech_update_resource_status(mpStatus=L, mpResource=promo_x_product);
	
	%tech_open_resource(mpResource=vf_load_data);
	
	*%tech_cas_session(mpMode = end
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
%mend vf001_load_data;