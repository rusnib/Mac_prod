%MACRO tech_redirect_log(mpMode=, mpJobName=, mpArea=);
	
	%local lmvMode
			lmvArea
			lmvJobName
	;
	
	%LET lmvMode=%upcase(&mpMode);
	%LET lmvArea=&mpArea;
	%LET lmvJobName = &mpJobName.;
	
	%M_ETL_GENERATE_DATE;
		
	%LET mvDATETIME=&mvDATETIME;
	
	DATA _NULL_;
		CALL SYMPUT("lmvArea", STRIP("&lmvArea"));
	RUN;
		
	%IF %LENGTH(&lmvArea.)>0 %THEN %DO;
		%LET mvOPT_AREA=&ETL_LOGS.;
		%LET mvOPT_LOGPATH=&lmvArea./&lmvJobName._&mvDATETIME..LOG;
	%END;

	%IF &lmvMode=START %THEN %DO;
		
		PROC PRINTTO LOG="&mvOPT_AREA./&mvOPT_LOGPATH." NEW;
		RUN;

	%END;
	%ELSE %IF &lmvMode=END %THEN %DO;

		PROC PRINTTO;
		RUN;
		
	%END;

%MEND tech_redirect_log;