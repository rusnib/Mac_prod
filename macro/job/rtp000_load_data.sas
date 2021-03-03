/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для подготовки данных в рамках сквозного процесса для оперпрогноза (продукты)
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
*     %rtp000_load_data;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro rtp000_load_data;
	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=pmix_sales_rtp);
	%tech_update_resource_status(mpStatus=P, mpResource=pbo_sales_rtp);
	%tech_update_resource_status(mpStatus=P, mpResource=product_rtp);
	%tech_update_resource_status(mpStatus=P, mpResource=cost_price_rtp);
	%tech_update_resource_status(mpStatus=P, mpResource=channel_rtp);
	%tech_update_resource_status(mpStatus=P, mpResource=comp_media_rtp);
	%tech_update_resource_status(mpStatus=P, mpResource=promo_rtp);
	%tech_update_resource_status(mpStatus=P, mpResource=promo_x_pbo_rtp);
	%tech_update_resource_status(mpStatus=P, mpResource=promo_x_product_rtp);
	%tech_update_resource_status(mpStatus=P, mpResource=rtp_abt_pmix_prepare_after_vf);
	
	
	/* Применение недельного профиля - переразбивка прогноза pmix до разреза месяц-флаг промо, прогноза gc - до разреза месяц*/
	%rtp_load_data_to_caslib(mpWorkCaslib=mn_short);

	%tech_update_resource_status(mpStatus=L, mpResource=pmix_sales_rtp);
	%tech_update_resource_status(mpStatus=L, mpResource=pbo_sales_rtp);
	%tech_update_resource_status(mpStatus=L, mpResource=product_rtp);
	%tech_update_resource_status(mpStatus=L, mpResource=cost_price_rtp);
	%tech_update_resource_status(mpStatus=L, mpResource=channel_rtp);
	%tech_update_resource_status(mpStatus=L, mpResource=comp_media_rtp);
	%tech_update_resource_status(mpStatus=L, mpResource=promo_rtp);
	%tech_update_resource_status(mpStatus=L, mpResource=promo_x_pbo_rtp);
	%tech_update_resource_status(mpStatus=L, mpResource=promo_x_product_rtp);
	%tech_update_resource_status(mpStatus=L, mpResource=rtp_abt_pmix_prepare_after_vf);
	
	%tech_open_resource(mpResource=rtp_load_data);
	%tech_open_resource(mpResource=rtp_abt_pbo_prepare);
	
	*%tech_cas_session(mpMode = end
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
%mend rtp000_load_data;