%macro fmk_load_etl_stg_hist(mpResource=);

	%local 
			lmvResource_src
			lmvResource_tgt
			lmvResId
			lmvResNm
			lmvResTypeLoad
			lmvFieldTmFrame
			lmvTmFrameVal
			lmvOutDtVarsCnt
			lmvOutDtVarsKeep
			lmvVersionId
			lmvEventId
			;
			
	%let lmvResource_src = %upcase(&mpResource._history);
	%let lmvResource_tgt = %upcase(&mpResource.);
	
	/* Проверка на валидность входных параметров (Сущ-е ресурса) */
	proc sql noprint;
		select 	resource_id
				,resource_nm 
		into 	:lmvResId
				,:lmvResNm
		from etl_cfg.cfg_resource
		where upcase(resource_nm) = "&lmvResource_tgt."
	;
	quit;
	%if %length(&lmvResNm.) eq 0 %then %do;
		%put ERROR: Invalid input parameters;
		%abort;
	%end;
	
	%let lmvKeep = %member_vars (etl_stg.stg_&lmvResource_tgt.);
	%let lmvKeepComma = %member_vars (etl_stg.stg_&lmvResource_tgt., mpDlm=%str(, ));

	proc sql noprint;
		create table clms as
		select *
		from sashelp.vcolumn 
		where upcase(libname) = 'ETL_STG' 
			and upcase(memname) = "STG_&lmvResource_tgt."
			and format = 'DATE9.'
		;
	quit;

	%let lmvOutDtVarsCnt = %member_obs (mpData=work.clms);

	%if &lmvOutDtVarsCnt. gt 0 %then %do;
		proc sql noprint;
			select name into :lmvOutDtVarsKeep separated by ' '
			from work.clms
			;
			select name into :lmvOutDtVarsNm1 %if &lmvOutDtVarsCnt. gt 1 %then %do; - :lmvOutDtVarsNm&lmvOutDtVarsCnt. %end; 
			from work.clms
			;
		quit;
	%end;
	
	proc sql noprint;
		connect using etl_cfg;
		execute by etl_cfg(
			insert into etl_cfg.cfg_resource_registry(
				event_id, resource_id, process_nm, extract_id, status_cd, exec_dttm)
			VALUES (DEFAULT, &lmvResId., %str(%')load_etl_stg_&lmvResource_src.%str(%'), DEFAULT, 'H', current_timestamp) )
	;
	quit;
	
	proc sql noprint;
		select extract_id into :lmvVersionId
		from etl_cfg.cfg_resource_registry
		where resource_id = &lmvResId.
			and exec_dttm = select max(exec_dttm) as max_dttm 
							from etl_cfg.cfg_resource_registry
							where resource_id = &lmvResId.
	;
	quit;
	
	%put &=lmvVersionId;
	
	
	data work.&lmvResource_tgt.(keep=&lmvKeep.);
	%if &lmvOutDtVarsCnt. gt 0 %then %do;
			format &lmvOutDtVarsKeep. date9.;
	%end;
	
		set IA.IA_&lmvResource_src.;
		%if &lmvOutDtVarsCnt. gt 0 %then %do;
			%do i=1 %to &lmvOutDtVarsCnt.;
				if &&lmvOutDtVarsNm&i..=. then do;
					&&lmvOutDtVarsNm&i..=.;
				end;
				else do;
					&&lmvOutDtVarsNm&i.. = datepart(&&lmvOutDtVarsNm&i..);
				end;
			%end;
		%end;
		etl_extract_id = &lmvVersionId.;
	run;
	
	proc sql noprint;
			connect using etl_stg;
			execute by etl_stg (
				truncate etl_stg.%lowcase(stg_&lmvResource_tgt.);
			);
	quit;
	%put table etl_stg.stg_&lmvResource_tgt. was truncated;
	
 	proc append base=ETL_STG.stg_&lmvResource_tgt.(bulkload=yes bl_default_dir="/data/pg_blk/" bl_psql_path="/usr/pgsql-11/bin/psql" BL_FORMAT=CSV BL_ESCAPE=ON BL_DELETE_DATAFILE=YES) data=work.&lmvResource_tgt. ;
    run;
	
		/* Calc uploaded/updated rows */
	%let lmvCntRowsTarget = %member_obs(mpData=work.&lmvResource_tgt.);
	
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
	
%mend fmk_load_etl_stg_hist;