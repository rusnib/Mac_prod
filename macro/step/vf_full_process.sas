/* ********************************************************************* */
/* ********************************************************************* */
/* Джоб для запуска сквозного процесса прогнозирования временными рядами */
/* ********************************************************************* */
/* ********************************************************************* */
%macro vf_full_process;
	/* 1. загрузка данных в CAS */
	/* Значения параметров mpEvents mpEventsMkup выставлены по умолчанию,
	*  если необходимо, значения можно изменить, но нужно учитывать, что таблица 
	*  параметра mpEventsMkup используется в:
	*  		vf_train_week_profile_gc(параметр mpInEventsMkup=)
	*  		vf_month_aggregation(параметр mpInEventsMkup=) */
	
	%tech_redirect_log(mpMode=START, mpJobName=vf_load_data, mpArea=Main);
	%tech_log_event(mpMode=START, mpProcess_Nm=vf_load_data);
		%vf_load_data(mpEvents=mn_long.events
					,mpEventsMkup=mn_long.events_mkup
					,mpOutLibref = mn_long
					,mpClearFlg=YES);
	%tech_log_event(mpMode=END, mpProcess_Nm=vf_load_data);
	%tech_redirect_log(mpMode=END, mpJobName=vf_load_data, mpArea=Main);
	
	%tech_redirect_log(mpMode=START, mpJobName=vf_restore_sales_gc, mpArea=Main);
	%tech_log_event(mpMode=START, mpProcess_Nm=vf_restore_sales_gc);
		%vf_restore_sales_gc;
	%tech_log_event(mpMode=END, mpProcess_Nm=vf_restore_sales_gc);
	%tech_redirect_log(mpMode=END, mpJobName=vf_restore_sales_gc, mpArea=Main);
	
	
	/*3. Загрузка таблицы pbo_sal_abt */
	/*	Значения параметров выставлены по умолчанию - если необходимо, их можно изменить, учитывая, 
	*	что они используются в vf_prepare_ts_abt_pmix(соответствующие параметры) */
	%tech_redirect_log(mpMode=START, mpJobName=vf_prepare_ts_abt_pbo, mpArea=Main);
	%tech_log_event(mpMode=START, mpProcess_Nm=vf_prepare_ts_abt_pbo);
		%vf_prepare_ts_abt_pbo(mpPboSalAbt=mn_long.pbo_sal_abt,
							mpPromoW1=mn_long.promo_w1,
							mpPromoD=mn_long.promo_d, 
							mpPboSales=mn_long.TS_pbo_sales,
							mpWeatherW=mn_long.weather_w);
	%tech_log_event(mpMode=END, mpProcess_Nm=vf_prepare_ts_abt_pbo);
	%tech_redirect_log(mpMode=END, mpJobName=vf_prepare_ts_abt_pbo, mpArea=Main);
	
	/*4. Запуск VF-проекта на основе pbo_sal_abt*/
	/* Необходимо указать ИМЯ VF-проекта. Например, pbo_sales_v2*/

	%tech_redirect_log(mpMode=START, mpJobName=vf_run_project_pbo, mpArea=Main);
	%tech_log_event(mpMode=START, mpProcess_Nm=vf_run_project_pbo);
		%vf_run_project(mpProjectName=&VF_PBO_PROJ_NM.); 
	%tech_log_event(mpMode=END, mpProcess_Nm=vf_run_project_pbo);
	%tech_redirect_log(mpMode=END, mpJobName=vf_run_project_pbo, mpArea=Main);
	
	/*5. Загрузка таблицы pmix_sal_abt*/
	/* Необходимо указать ИМЯ VF-проекта в параметре mpProjectName, построенного на mpPboSalAbt=casuser.pbo_sal_abt */
	%tech_redirect_log(mpMode=START, mpJobName=vf_prepare_ts_abt_pmix, mpArea=Main);
	%tech_log_event(mpMode=START, mpProcess_Nm=vf_prepare_ts_abt_pmix);
		%vf_prepare_ts_abt_pmix(mpVfPboProjName=&VF_PBO_PROJ_NM.,
									mpPmixSalAbt=mn_long.pmix_sal_abt,
									mpPromoW1=mn_long.promo_w1,
									mpPromoD=mn_long.promo_d,
									mpPboSales=mn_long.TS_pbo_sales,
									mpWeatherW=mn_long.weather_w,
									mpAuth = NO);
	%tech_log_event(mpMode=END, mpProcess_Nm=vf_prepare_ts_abt_pmix);
	%tech_redirect_log(mpMode=END, mpJobName=vf_prepare_ts_abt_pmix, mpArea=Main);
	
	/*6. Запуск VF-проекта на основе pmix_sal_abt*/
	/* Необходимо указать ИМЯ VF-проекта в параметре mpProjectName. Например, pmix_sales_v1*/
	%tech_redirect_log(mpMode=START, mpJobName=vf_run_project_pmix, mpArea=Main);
	%tech_log_event(mpMode=START, mpProcess_Nm=vf_run_project_pmix);
 		%vf_run_project(mpProjectName=&VF_PMIX_PROJ_NM.); 
 	%tech_log_event(mpMode=END, mpProcess_Nm=vf_run_project_pmix);
	%tech_redirect_log(mpMode=END, mpJobName=vf_run_project_pmix, mpArea=Main);
	/*7. Создание модели недельного профиля для разбивки по дням и переагрегации недель до месяцев*/
	%tech_redirect_log(mpMode=START, mpJobName=vf_train_week_profile, mpArea=Main);
	%tech_log_event(mpMode=START, mpProcess_Nm=vf_train_week_profile);
		%vf_train_week_profile(mpOutWpGc=mn_dict.wp_gc);
	%tech_log_event(mpMode=END, mpProcess_Nm=vf_train_week_profile);
	%tech_redirect_log(mpMode=END, mpJobName=vf_train_week_profile, mpArea=Main);
	
	/*7. Создание модели недельного профиля для разбивки GC по дням и переагрегации недель до месяцев*/
	/*%vf_train_week_profile_gc(mpInEventsMkup=dm_abt.events_mkup,
									 mpNnetWp=casuser.nnet_wp1,
									 mpPromo_W=casuser.promo_w 
									 ); */
	/* Применение недельного профиля - переразбивка прогноза pmix до разреза месяц-флаг промо, прогноза gc - до разреза месяц*/
	/* Параметры mpPrmt=Y/N (Будут ли указанные таблицы запромоучены) */
	/* Параметр mpInWpGc = таблица, формируемая в vf_train_week_profile(параметр mpOutWpGc); */
	/*8. Необходимо указать ИМЕНА VF-проектов в параметрах mpVfPmixProjName, mpVfPboProjName*/
	*%tech_redirect_log(mpMode=START, mpJobName=vf_month_aggregation, mpArea=Main);
	*%tech_log_event(mpMode=START, mpProcess_Nm=vf_month_aggregation);
		*%vf_month_aggregation(mpVfPmixProjName=&VF_PMIX_PROJ_NM.,
								mpVfPboProjName=&VF_PBO_PROJ_NM.,
								mpInEventsMkup=mn_long.events_mkup,
								mpOutPmix=mn_long.plan_pmix_month,
								mpOutGc=mn_long.plan_gc_month, 
								mpOutOutforgc=mn_long.TS_OUTFORGC,
								mpOutOutfor=mn_long.TS_OUTFOR, 
								mpOutNnetWp=mn_dict.nnet_wp1,
								mpInWpGc=mn_dict.wp_gc,
								mpPrmt=Y) ;
	*%tech_log_event(mpMode=END, mpProcess_Nm=vf_month_aggregation);
	*%tech_redirect_log(mpMode=END, mpJobName=vf_month_aggregation, mpArea=Main);
	
	/* 9. Выгрузка данных в CSV + в DP */
	*%vf_6_out_integration(mpVfPmixProjName=pmix_sales_v1,
								mpVfPboProjName=pbo_sales_v1,
								mpMLPmixTabName=DM_ABT.PLAN_PMIX_MONTH,
								mpInEventsMkup=dm_abt.events_mkup,
								mpInWpGc=mn_dict.wp_gc,
								mpOutPmixLt=casuser.plan_pmix_month,
								mpOutGcLt=casuser.plan_gc_month, 
								mpOutUptLt=casuser.plan_upt_month, 
								mpOutOutforgc=casuser.TS_OUTFORGC,
								mpOutOutfor=casuser.TS_OUTFOR, 
								mpOutNnetWp=mn_dict.nnet_wp1,
								mpPrmt=N) ;
%mend vf_full_process;