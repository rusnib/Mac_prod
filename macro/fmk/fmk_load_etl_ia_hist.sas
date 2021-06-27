%macro fmk_load_etl_ia_hist(mpResource=);

	%local lmvResource
			lmvResId
			lmvResNm
	;
	%let lmvResource = %lowcase(&mpResource.);
	/* Проверка на валидность входных параметров (Сущ-е ресурса) */
	proc sql noprint;
		select 	resource_id
				,resource_nm 
		into 	:lmvResId
				,:lmvResNm
		from etl_cfg.cfg_resource
		where lowcase(resource_nm) = "&lmvResource."
	;
	quit;
	%if %length(&lmvResNm.) eq 0 %then %do;
		%put ERROR: Invalid input parameters;
		%abort;
	%end;
	
	/* Забираем самую свежую выгрузку */
	proc sql noprint;
		create table work.tmp_&lmvResource._delta as
			select * 
					,'N' as etl_delta_cd length=1
					,&ETL_CURRENT_DTTM. as valid_from_dttm length=8 format=datetime24.
					,&ETL_SCD_FUTURE_DTTM. as valid_to_dttm length=8 format=datetime24.
			from etl_stg.stg_&lmvResource.
			where etl_extract_id = select extract_id						
									from etl_cfg.cfg_resource_registry 
									where exec_dttm = select max(exec_dttm) as max 
														from etl_cfg.cfg_resource_registry 
														where resource_id = &lmvResId.
													/*	and status_cd in ('H', 'E')  */
	;													
	quit;
	
	/* Нет данных новых - выходим */
	%if %member_obs(mpData=work.tmp_&lmvResource._delta) eq 0 %then %do;
		%put WARNING: There are no data to load (Fresh extract_id does not exist) for resource = &lmvResource. (&lmvResId.) 
		in etl_stg.stg_&lmvResource.. Check out etl_cfg.cfg_resource_registry  for existence of new snapshot.;
		%return;
	%end;

	/* Очищаем таблицы */
	proc sql noprint;
		connect using etl_ia;
		execute by etl_ia (
			truncate etl_ia.&lmvResource._snap;
			truncate etl_ia.&lmvResource._snup;
			truncate etl_ia.&lmvResource._delta;
			truncate etl_ia.&lmvResource.;
		);
	quit;
	
	/*Загружаем данные в основную таблицу*/
	proc append base=etl_ia.&lmvResource.(bulkload=yes bl_default_dir="/data/pg_blk/" bl_psql_path="/usr/pgsql-11/bin/psql" BL_FORMAT=CSV BL_ESCAPE=ON BL_DELETE_DATAFILE=YES) data=work.tmp_&lmvResource._delta(drop=etl_delta_cd) force;
	run;

	/* Calc uploaded/updated rows */
	%let lmvCntRowsTarget = %member_obs(mpData=work.tmp_&lmvResource._delta);
	
	/*  Обновляем конфиг таблицу */
	proc sql noprint;
		connect using etl_cfg;
		execute by etl_cfg(
		update etl_cfg.cfg_resource_registry 
		set status_cd = 'L'
			,uploaded_from_source=&lmvCntRowsTarget.
			,uploaded_to_target=&lmvCntRowsTarget.		
		where exec_dttm = (select max(exec_dttm) as max
					from etl_cfg.cfg_resource_registry where resource_id = &lmvResId. and status_cd in ('H'))
				and resource_id = &lmvResId. and status_cd in ('H');
		)
	;
	quit;
	
%mend fmk_load_etl_ia_hist;