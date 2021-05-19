/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для создания модели недельного профиля для 
*	  разбивки GC по дням и переагрегации недель до месяцев
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
*     %vf009_week_profile_gc;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro vf009_week_profile_gc;
	
	%tech_log_event(mpMode=START, mpProcess_Nm=vf_train_week_profile_gc);

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_update_resource_status(mpStatus=P, mpResource=vf_train_week_profile);
	
	/* Применение недельного профиля - переразбивка прогноза pmix до разреза месяц-флаг промо, прогноза gc - до разреза месяц*/
	%vf_train_week_profile_gc(mpInEventsMkup=mn_dict.events_mkup,
									 mpNnetWp=mn_dict.nnet_wp1,
									 mpPromo_W=mn_dict.promo_w 
									 );

	%tech_update_resource_status(mpStatus=L, mpResource=vf_train_week_profile);
	
	%tech_open_resource(mpResource=vf_month_aggregation);
	
	%tech_log_event(mpMode=END, mpProcess_Nm=vf_train_week_profile_gc);
	
%mend vf009_week_profile_gc;