%MACRO tech_open_resource(mpResource=);
	%local lmvResource
		   lmvResCheck
			;
	%LET lmvResource=%sysfunc(upcase(&mpResource.));
	
	DATA _NULL_;
		lmvResource="&lmvResource.";
		CALL SYMPUT ("lmvResource", STRIP(lmvResource));
	RUN;
	
	PROC SQL NOPRINT;
		SELECT resource_nm into :lmvResCheck
		FROM etl_cfg.cfg_status_table
		WHERE UPCASE(STRIP(RESOURCE_NM))=UPCASE(STRIP("&lmvResource."));
	QUIT;
	
	%if %length(&lmvResCheck.) gt 0 %then %do;
		%put WARNING: Resource &lmvResource. already exists in table etl_cfg.cfg_status_table;
		%return;
	%end;
	
	PROC SQL NOPRINT;
		CREATE TABLE DATA_TO_APPEND AS
		SELECT
			RESOURCE_ID,
			RESOURCE_NM,
			"A" AS STATUS_CD,
			DHMS(today(), 00, 00, 00) AS PROCESSED_DTTM,
			0 as retries_cnt
		FROM
			etl_cfg.cfg_resource
		WHERE
			UPCASE(STRIP(RESOURCE_NM))=UPCASE(STRIP("&lmvResource."))
		;
	QUIT;
	
	%cmn_append_data(mpData=DATA_TO_APPEND, mpBase=etl_cfg.cfg_status_table);
	
%MEND tech_open_resource;