%MACRO tech_job_details(mpMODE=, mpSTEP_TYPE=);
	%LET mvMODE = %upcase(&mpMODE.);
	%LET mvSTEP_TYPE = %upcase(&mpSTEP_TYPE.);

	%LOCAL mvSTEP_COUNT;
	%GLOBAL
		mvNEXT_STEP
		mvJOB_ID
	;

	%IF &mvMODE.=START %THEN %DO;
		PROC SQL NOPRINT;
			connect using etl_cfg;
			SELECT ev_id INTO :mvJOB_ID
			FROM CONNECTION TO etl_cfg
				(
					SELECT MAX(event_id) as ev_id
					FROM etl_cfg.cfg_log_event
					WHERE
						process_nm = %STR(%')&mvPROCESS_NM.%STR(%') AND
						process_id = &mvPROCESS_ID.
				);
		QUIT;

		PROC SQL NOPRINT;
			connect using etl_cfg;
			SELECT step_count INTO :mvSTEP_COUNT
			FROM CONNECTION TO etl_cfg
				(
					SELECT COUNT(*) as step_count
					FROM etl_cfg.cfg_job_details
					WHERE job_id = &mvJOB_ID.;
				);
		QUIT;

		%LET mvNEXT_STEP = %SYSEVALF(&mvSTEP_COUNT. + 1);

		PROC SQL NOPRINT;
			connect using etl_cfg;
			EXECUTE BY etl_cfg
				(
					INSERT INTO etl_cfg.cfg_job_details
					VALUES
						(
							&mvJOB_ID.,
							&mvNEXT_STEP.,
							%STR(%')&mvSTEP_TYPE.%STR(%'),
							NOW(),
							NULL,
							'',
							'',
							'',
							NULL
						)
				);
		QUIT;
	%END;
	%ELSE %IF &mvMODE.=END %THEN %DO;
		%IF &SYSCC>4 %THEN %DO;
			OPTIONS NOSYNTAXCHECK OBS=MAX;
		%END;

		%IF &SYSCC>4 %THEN %DO;
			%LET mvPROCESS_STATUS=Step has finished with ERROR;
			%LET mvERR_TEXT=%SYSFUNC(COMPRESS(%SYSFUNC(TRANWRD(ERROR: %NRQUOTE(&SYSERRORTEXT.), %STR(,), %STR(:))), %STR(''%STR(%')%STR(%'))));
			%IF %LENGTH(&mvERR_TEXT.)>100 %THEN %DO;
				%LET mvERR_LEN=100;
			%END;
			%ELSE %DO;
				%LET mvERR_LEN=%LENGTH(&mvERR_TEXT.);
			%END;
			%LET mvSTATUS_DESCRIPTION=%SUBSTR(%STR(&mvERR_TEXT.), 1, &mvERR_LEN.);
		%END;
		%ELSE %DO;
			%LET mvPROCESS_STATUS=Step has finished SUCCESSFULLY;
			%LET mvSTATUS_DESCRIPTION=;
		%END;

		PROC SQL NOPRINT;
			connect using etl_cfg;
			EXECUTE BY etl_cfg
				(
					UPDATE etl_cfg.cfg_job_details
					SET
						proc_status=%STR(%')&mvPROCESS_STATUS.%STR(%'),
						status_description=%STR(%')&mvSTATUS_DESCRIPTION.%STR(%'),
						end_dttm=NOW(),
						object_name=%STR(%')&SYSLAST.%STR(%'),
						nobs=&SYSNOBS.
					WHERE
						job_id=&mvJOB_ID. AND
						step_id=&mvNEXT_STEP.
				);
		QUIT;

		%IF &SYSCC.>4 %THEN %DO;
			%ABORT abend;
		%END;
	%END;
%MEND tech_job_details;