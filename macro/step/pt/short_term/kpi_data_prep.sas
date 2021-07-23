%macro assign;
%let casauto_ok = %sysfunc(SESSFOUND ( cmasauto)) ;
%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
 cas casauto;
 caslib _all_ assign;
%end;
%mend;
%assign

options casdatalimit=600000M;

%let lmvReportDttm 	       = &ETL_CURRENT_DTTM.;

/* Поднятие в CAS истории чеков */
proc casutil;
	droptable 
		casdata		= "PBO_SALES" 
		incaslib	= "MAX_CASL" 
		quiet         
	;                 
run;                  
data MAX_CASL.PBO_SALES (replace=yes promote=yes drop=valid_from_dttm valid_to_dttm);
	set ETL_IA.PBO_SALES (where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
run;

/* Поднятие в CAS истории юнитов */
proc casutil;
	droptable 
		casdata		= "PMIX_SALES" 
		incaslib	= "MAX_CASL" 
		quiet         
	;                 
run;                  
data MAX_CASL.PMIX_SALES (replace=yes promote=yes drop=valid_from_dttm valid_to_dttm);
	set ETL_IA.PMIX_SALES (where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
run;


/* Поднятие в CAS справочников*/
/* PBO_CLOSE_PERIOD */
proc casutil;
droptable 
	casdata		= "PBO_CLOSE_PERIOD" 
	incaslib	= "MAX_CASL" 
	quiet         
;                 
run; 
data MAX_CASL.PBO_CLOSE_PERIOD (replace=yes drop=valid_from_dttm valid_to_dttm);
	set ETL_IA.PBO_CLOSE_PERIOD (where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
run;

/* DICTIONARIES */
%let common_path = /opt/sas/mcd_config/macro/step/pt/alerts;
%include "&common_path./data_prep_product.sas"; 
%data_prep_product(
	  mpInLib 		= ETL_IA
	, mpReportDttm 	= &ETL_CURRENT_DTTM.
	, mpOutCasTable = CASUSER.PRODUCT_DICTIONARY
);
proc casutil;
	droptable 
		casdata		= "PRODUCT_DICTIONARY" 
		incaslib	= "MAX_CASL" 
		quiet         
	;                 
run; 
data MAX_CASL.PRODUCT_DICTIONARY (promote=yes);
	set CASUSER.PRODUCT_DICTIONARY;
run;

%include "&common_path./data_prep_pbo.sas"; 
%data_prep_pbo(
	  mpInLib 		= ETL_IA
	, mpReportDttm 	= &ETL_CURRENT_DTTM.
	, mpOutCasTable = CASUSER.PBO_DICTIONARY
);
proc casutil;
	droptable 
		casdata		= "PBO_DICTIONARY" 
		incaslib	= "MAX_CASL" 
		quiet         
	;                 
run; 
data MAX_CASL.PBO_DICTIONARY (promote=yes);
	set CASUSER.PBO_DICTIONARY;
run;


%add_promotool_marks2(mpOutCaslib=casuser,
							mpPtCaslib=pt,
							PromoCalculationRk=);
proc casutil;
	droptable 
		casdata		= "PRODUCT_CHAIN_ENH" 
		incaslib	= "MAX_CASL" 
		quiet         
	;                 
run; 
data MAX_CASL.PRODUCT_CHAIN_ENH (promote=yes);
	set CASUSER.PRODUCT_CHAIN_ENH;
run;