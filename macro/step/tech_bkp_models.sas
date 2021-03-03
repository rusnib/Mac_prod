%macro tech_bkp_models(mpModelTable=PMIX_MODEL_TABLE);
	%local lmvCnt
			;
	data models;
		set models.&mpModelTable.;
	run;
	
	data _null_;
		set models end=_end;
		if _end then call symputx('lmvCnt',_n_);
	run;
	
	%do i=1 %to &lmvCnt;
		data _null_;
			set models (obs=&i. firstobs=&i.);
			call symputx('model',model);
		run;

		proc astore;
			download RSTORE=MODELS.&model store="/data/ETL_BKP/&model..ast";
		run;
	%end;
	
%mend tech_bkp_models;