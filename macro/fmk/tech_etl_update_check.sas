%macro tech_etl_update_check;


	%local
		lmvResList        	/* список ресурсов из ETL_IA */
		lmvResListLength	/* кол-во ресурсов */
		lmvResMeanActual	/* среднее число актуальных строк ресурса */
		lmvResMeanUpdated	/* среднее число обновленных строк ресурса */
		lmvResUclmActual	/* верхний порог 90% данных для актуальных строк */
		lmvResLclmActual	/* нижний порог 90% данных для актуальных строк */
		lmvResUclmUpdated	/* верхний порог 90% данных для обновленных строк */
		lmvResLclmUpdated	/* нижний порог 90% данных для обновленных строк */
		lmvResLastUpdated	/* обновлённые строки последней выгрузки */
		lmvResLastActual	/* актуальные строки последней выгрузки */
		lmvBotMessage		/* сообщение для бота */
		;

	/* Получить список ресурсов etl*/
	proc sql noprint;
		SELECT
			resource_nm INTO :lmvResList separated by ' '
		FROM ETL_CFG.CFG_RESOURCE
		WHERE module_nm = 'etl_ia';
	quit;
	
	%let lmvResList = %upcase(&lmvResList.);
	%let lmvResListLength = %sysfunc(countw(&lmvResList., %str( )));
	
	/* Для каждого ресурса провести проверку данных */
	%do i=1 %to &lmvResListLength.;
		%let lmvResource = %scan(&lmvResList., &i., %str( ));
		/* Получить все результаты выгрузки etl_ia, оставив только числовые данные о кол-ве обновленных строк и актуальных строк */
		proc sql noprint;
			CREATE TABLE WORK.updts_resource AS
			SELECT
				 updated
				,actual_count
			FROM etl_cfg.cfg_resource_registry
			WHERE UPCASE(process_nm) = UPCASE("load_etl_stg_&lmvResource.")
				AND status_cd = 'L'
				AND updated IS NOT NULL
				AND actual_count IS NOT NULL;
		quit;
		
		data work.resource_stats_act (keep=actual_count);
			set work.updts_resource;
		run;
		
		data work.resource_stats_upd (keep=updated);
			set work.updts_resource;
		run;
		
		proc means data=work.resource_stats_act alpha=0.1 maxdec=2 clm noprint;
			var actual_count;
			output out=work.resource_act_clm
					UCLM=UCLM
					LCLM=LCLM;
		run;
		
		proc means data=work.resource_stats_upd alpha=0.1 maxdec=2 clm noprint;
			var updated;
			output out=work.resource_upd_clm
				UCLM=UCLM
				LCLM=LCLM;
		run;
		
		proc sql noprint;
			SELECT
				 UCLM
				,LCLM
			INTO :lmvResUclmActual, :lmvResLclmActual
			FROM WORK.resource_act_clm;
			
			SELECT
				 UCLM
				,LCLM
			INTO :lmvResUclmUpdated, :lmvResLclmUpdated
			FROM WORK.resource_upd_clm;
		quit;
		
		
		/* Сделать фильтрацию данных, оставив только 90% всей выборки */
		data work.resource_stats_act (replace=yes);
			set work.resource_stats_act;
			where= (&lmvResLclmActual. <= actual_count) and (actual_count <= &lmvResUclmActual.);
		run;
		
		data work.resource_stats_upd (replace=yes);
			set work.resource_stats_upd;
			where= ((&lmvResLclmUpdated. <= updated) and (updated <= &lmvResUclmUpdated.));
		run;
		
		
		/* Снова посчитать среднее значение */
		proc sql noprint;
			SELECT MEAN(updated)
			INTO :lmvResMeanUpdated
			FROM work.resource_stats_upd;
			
			SELECT MEAN(actual_count)
			INTO :lmvResMeanActual
			FROM work.resource_stats_act;
		quit;
			
		/* Получить последнюю запись о выгрузке (кол-во обновленных и актуальных строк) */
		proc sql noprint;
			SELECT
				 updated
				,actual_count
			INTO :lmvResLastUpdated, :lmvResLastActual
			FROM ETL_CFG.cfg_resource_registry
			WHERE UPCASE(process_nm) = UPCASE("load_etl_stg_&lmvResource.")
				AND status_cd = "L"
				AND datepart(exec_dttm) = datepart((
					SELECT MAX(exec_dttm)
					FROM ETL_CFG.cfg_resource_registry
					WHERE UPCASE(process_nm) = UPCASE("load_etl_stg_&lmvResource.")
						AND status_cd = "L"
			));
		quit;

		%let lmvActualDiv = %sysevalf(&lmvResLastActual. / &lmvResMeanActual.);
		%let lmvUpdatedDiv = %sysevalf(&lmvResLastUpdated. / &lmvResMeanUpdated.);
		%let lmvBotMessage =;

		/* Если значения по последней выгрузке больше среднего на 20% */
		
		%if &lmvUpdatedDiv. gt 1.2 %then %do;
			%let lmvBotMessage = &lmvBotMessage. | Value of updated rows &lmvResLastUpdated. (mean of updated: &lmvResMeanUpdated.);
		%end;
		
		%if &lmvActualDiv gt 1.2 %then %do;	
			/* Добавить соответствующий текст в сообщение боту */
			%let lmvBotMessage = &lmvBotMessage. | Value of actual rows &lmvResLastActual. (mean of actual: &lmvResMeanActual.);
		%end;
		
		
		
		%put &=lmvBotMessage;
		
		/* Если сообщение боту по длине не 0 */
		%if %length(&lmvBotMessage.) gt 0 %then %do;
			%let lmvBotMessage = WARNING (&lmvResource.) &lmvBotMessage. ;
			
			filename resp temp ;
			proc http 
				 method="POST"
				 url="https://api.telegram.org/bot&TG_BOT_TOKEN./sendMessage?chat_id=-1001360913796&text=&lmvBotMessage."
				 ct="application/json"
				 out=resp; 
			run;
			/* Отправить сообщение боту */
		%end;
		
		proc datasets lib=work nolist;
			delete updts_resource resource_stats_act resource_stats_upd resource_act_clm resource_upd_clm resource_stats_act resource_stats_upd;
		run;
		quit;
		
	%end;
%mend tech_etl_update_check;
