/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для загрузки таблицы pmix_sal_abt в рамках 
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
*     %vf005_prepare_ts_abt_pmix;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf005_prepare_ts_abt_pmix;

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=vf_run_project_pbo);
	/* 4. Загрузка таблицы pmix_sal_abt*/
	%vf_prepare_ts_abt_pmix(mpVfPboProjName=&VF_PBO_PROJ_NM.,
								mpPmixSalAbt=mn_long.pmix_sal_abt,
								mpPromoW1=mn_long.promo_w1,
								mpPromoD=mn_long.promo_d,
								mpPboSales=mn_long.TS_pbo_sales,
								mpWeatherW=mn_long.weather_w,
								mpAuth = YES);

	%tech_update_resource_status(mpStatus=L, mpResource=vf_run_project_pbo);
	
	%tech_open_resource(mpResource=vf_prepare_ts_abt_pmix);
	
	*%tech_cas_session(mpMode = end
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
	
%mend vf005_prepare_ts_abt_pmix;