/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для применения недельного профиля - переразбивка прогноза 
*	  pmix до разреза месяц-флаг промо, прогноза gc - до разреза месяц
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
*     %vf007_month_aggregation;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf007_month_aggregation;

	%tech_log_event(mpMode=START, mpProcess_Nm=vf_month_aggregation);

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	/* Получение токена аутентификации */
	%tech_get_token(mpUsername=&SYS_ADM_USER., mpOutToken=tmp_token);
	
	%tech_update_resource_status(mpStatus=P, mpResource=vf_train_week_profile);
	
	/* Применение недельного профиля - переразбивка прогноза pmix до разреза месяц-флаг промо, прогноза gc - до разреза месяц*/
	%vf_month_aggregation(mpVfPmixProjName=&VF_PMIX_PROJ_NM.,
								mpVfPboProjName=&VF_PBO_PROJ_NM.,
								mpInEventsMkup=mn_long.events_mkup,
								mpOutPmix=mn_long.plan_pmix_month,
								mpOutGc=mn_long.plan_gc_month, 
								mpOutOutforgc=mn_long.TS_OUTFORGC,
								mpOutOutfor=mn_long.TS_OUTFOR, 
								mpOutNnetWp=mn_dict.nnet_wp1,
								mpInWpGc=mn_dict.wp_gc,
								mpPrmt=Y,
								mpAuth = YES);

	%tech_update_resource_status(mpStatus=L, mpResource=vf_train_week_profile);
	
	%tech_open_resource(mpResource=vf_month_aggregation);
	%tech_open_resource(mpResource=rtp_abt_pmix_prepare_after_vf);
	
	%tech_log_event(mpMode=END, mpProcess_Nm=vf_month_aggregation);
	
%mend vf007_month_aggregation;