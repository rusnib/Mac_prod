/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для получения таргет таблиц с прогнозами
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
*     %rtp009_out_integration;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro rtp009_out_integration;

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
	%tech_log_event(mpMode=START, mpProcess_Nm=rtp_7_out_integration);				
	%tech_update_resource_status(mpStatus=P, mpResource=rtp_score_pmix);
	/* Получение токена аутентификации */
	%tech_get_token(mpUsername=&SYS_ADM_USER., mpOutToken=tmp_token);
		%rtp_7_out_integration(mpVfPmixProjName=&VF_PMIX_PROJ_NM.,
									mpVfPboProjName=&VF_PBO_PROJ_NM.,
									mpMLPmixTabName=mn_short.pmix_days_result,
									mpInEventsMkup=mn_long.events_mkup,
									mpInWpGc=mn_dict.wp_gc,
									mpOutPmixLt=mn_short.plan_pmix_month,
									mpOutGcLt=mn_short.plan_gc_month, 
									mpOutUptLt=mn_short.plan_upt_month, 
									mpOutPmixSt=mn_short.plan_pmix_day,
									mpOutGcSt=mn_short.plan_gc_day, 
									mpOutUptSt=mn_short.plan_upt_day, 
									mpOutOutforgc=mn_short.TS_OUTFORGC,
									mpOutOutfor=mn_short.TS_OUTFOR, 
									mpOutNnetWp=mn_dict.nnet_wp1,
									mpPrmt=Y,
									mpInLibref=mn_short,
									mpAuth = YES);
	
	
	%tech_update_resource_status(mpStatus=L, mpResource=rtp_score_pmix);
	%tech_open_resource(mpResource=rtp_out_integration);
	%tech_log_event(mpMode=END, mpProcess_Nm=rtp_7_out_integration);	
%mend rtp009_out_integration;