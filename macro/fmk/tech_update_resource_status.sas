%MACRO tech_update_resource_status(mpStatus=, mpResource=);
	%local lmvStatus
			lmvResource
			lmvPrevSYSCC
			;
			
	%LET lmvStatus=%sysfunc(upcase(&mpStatus.));
	%LET lmvResource=%sysfunc(upcase(&mpResource.));
	%LET lmvPrevSYSCC=0;
	
	%IF &SYSCC>4 %THEN %DO;
		/* Если произошла ошибка - помечаем статус ресурса E */
		%LET lmvStatus=E;
		
		%LET lmvPrevSYSCC=&SYSCC.;
		%LET SYSCC=0;
		
		/**/
		OPTIONS NOSYNTAXCHECK OBS=MAX;
		
	%END;
	
	PROC SQL NOPRINT;
		UPDATE etl_cfg.cfg_status_table
		SET
			STATUS_CD="&lmvStatus."
		WHERE
				/*(UPCASE(STRIP(RESOURCE_NM))=STRIP("&lmvResource.") AND BATCH_CYCLE_ID=(select max(batch_id) from etl_cfg.cfg_batch_etl_cycle)) 
				or*/
				(UPCASE(STRIP(RESOURCE_NM))=STRIP("&lmvResource.") and BATCH_CYCLE_ID = . AND UPCASE(STRIP(STATUS_CD)) ^="C")
		;
	QUIT;
	
	%IF &lmvPrevSYSCC.>0 %THEN %DO;
	
		%LET SYSCC=&lmvPrevSYSCC.;
		
		OPTIONS SYNTAXCHECK OBS=0;
		
	%END;
		
%MEND tech_update_resource_status;
