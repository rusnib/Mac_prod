%macro last_updates_ia(mpTABLE_NM=, mpDEEP_LVL=5);
	%local
		lmvTable_nm
		lmvDeep_level;
	
	%global
		LAST_UPDATES_IA;
		
	%let lmvTable_nm = &mpTABLE_NM.;
	%let lmvDeep_level = &mpDEEP_LVL.;
	
	PROC SQL NOPRINT OUTOBS=&lmvDeep_level.;
		SELECT 
			DISTINCT etl_extract_id FORMAT=8. as extr_id INTO :LAST_UPDATES_IA separated by ','
		FROM etl_ia.&lmvTable_nm.
		ORDER BY extr_id DESC;
	QUIT;
%mend last_updates_ia;