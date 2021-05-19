%macro fmk_load_etl_ia_error_check(mpResId = &lmvResId., mpResource = &lmvResource.);
	%if &SYSCC gt 4 %then %do;
		/* Return session in execution mode */
		OPTIONS NOSYNTAXCHECK OBS=MAX;
		proc sql noprint;
			connect using etl_cfg;
			execute by etl_cfg(
				update etl_cfg.cfg_resource_registry
				set status_cd='E'
				where resource_id = &mpResId. and status_cd in ('P') and  uploaded_to_target is null;
			);
		quit;
		
		%put ERROR: &mpResource. was uploaded unsuccessfully: %SYSFUNC(COMPRESS(%SYSFUNC(TRANWRD(ERROR: %NRQUOTE(&SYSERRORTEXT.), %STR(,), %STR(:))), %STR(''"")))!;
		%abort;
	%end;
%mend fmk_load_etl_ia_error_check;