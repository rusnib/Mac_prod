%macro hold_only_top_ia(mpTABLE_NM=, mpDEEP_LVL=5);
	%local
		lmvTable_nm
		lmvDeep_level;

	%let lmvTable_nm = &mpTABLE_NM.;
	%let lmvDeep_level = &mpDEEP_LVL;

	%last_updates_ia(mpTABLE_NM=&lmvTable_nm., mpDEEP_LVL=&lmvDeep_level.);

	PROC SQL NOPRINT;
		connect using etl_ia;
		EXECUTE BY etl_ia
			(
				DELETE FROM etl_ia.&lmvTable_nm.
				WHERE
					etl_extract_id NOT IN (&LAST_UPDATES_IA.)
			)
	QUIT;
%mend hold_only_top_ia;