/*************************************************/
/* Параметры :	*/
/* mpMode = delta/snap/snup */
/* mpTargetTableNm = product */
/* mpInputTableNm = work.product_delta */
/*************************************************/

%macro cmn_load_etl_ia_artefacts(mpMode=
								,mpTargetTableNm=
								,mpInputTableNm=
								);
	%local lmvMode
			lmvTargetTableNm
			lmvInputTableNm
			;
			
	%let lmvMode = %lowcase(&mpMode.);
	%let lmvTargetTableNm = %lowcase(&mpTargetTableNm.);
	%let lmvInputTableNm = %lowcase(&mpInputTableNm.);
	
	/* Проверка на валидность входных параметров (Сущ-е ресурса) */
	%if not %sysfunc(exist(etl_ia.&lmvTargetTableNm._&lmvMode.)) %then %do;
		%put ERROR: Current table does not exist : "&lmvTargetTableNm._&lmvMode.";
		%abort;
	%end;
	/* Проверка на валидность входных параметров (Сущ-е ресурса) */
	%if not %sysfunc(exist(&lmvInputTableNm.)) %then %do;
		%put ERROR: Current table does not exist : "&lmvInputTableNm.";
		%abort;
	%end;
	
	/* Очищаем артефакт */
	proc sql noprint;
		connect using etl_ia;
		execute by etl_ia (
			truncate etl_ia.&lmvTargetTableNm._&lmvMode.;
		);
	quit;
		
	%let lmvKeep = %member_vars (etl_ia.&lmvTargetTableNm._&lmvMode.);
	
	data work.tmp_&lmvTargetTableNm._&lmvMode.(keep=&lmvKeep.);
		length etl_digest1_cd etl_digest2_cd $256 etl_delta_cd $1;
		set &lmvInputTableNm.;
		call missing(etl_digest1_cd, etl_digest2_cd, etl_delta_cd);
	run;
	
	/*Загружаем данные в артефакт */
	proc append base=etl_ia.&lmvTargetTableNm._&lmvMode.(&ETL_PG_BULKLOAD.) data=work.tmp_&lmvTargetTableNm._&lmvMode. force;
	run;
	
%mend cmn_load_etl_ia_artefacts;