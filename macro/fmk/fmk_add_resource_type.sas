/*
	АРГУМЕНТЫ:
		mpResourceNm - Наименование ресурса, для которого будут создаваться таблицы
		mpPkList - Cписок первичных ключей таблицы, которые должны быть учтены при создании таблицы
*/
%macro fmk_add_resource_type(mpResourceNm=, mpPkList=);
	%local
		lmvResourceNm
		lmvPkList
		lmvClmnList
		lmvChrClmnList
		lmvChrClmnLengthList
		lmvNumericClmnList
		lmvDtList
		;
		
		
	%let lmvResourceNm = %upcase(&mpResourceNm.);
	%let lmvPkList = %upcase(&mpPkList.);
	
	/* Проверка на существование данного ресурса в интегрослое */
	proc sql noprint;
		SELECT name INTO :lmvClmnList separated by ' '
		FROM sashelp.vcolumn
		WHERE libname='IA' AND memname="IA_&lmvResourceNm.";
	quit;
	
	%if %length(&lmvClmnList.) eq 0 %then %do;
		%put ERROR: Resource &lmvResourceNm does not exist in IA;
		%abort;
	%end;
	
	/*Проверка наличия колонок из mpPkList в искомой таблице*/
	%let lmvCntPk = %sysfunc(countw(&lmvPkList., %str( )));
	%do i=1 %to &lmvCntPk.;
		%let lmvPkName = %scan(&lmvPkList, &i., %str( ));
		%if %index(&lmvClmnList., &lmvPkName.) eq 0 %then %do;
			%put ERROR: Primary key (&lmvPkName.) does not exist in resource &lmvResourceNm.;
			%abort;
		%end;
	%end;
	
	/* Создание таблицы в ETL_STG */
	proc sql noprint;
		CREATE TABLE ETL_STG.STG_&lmvResourceNm. 
		LIKE IA.IA_&lmvResourceNm.;
	quit;
	
	/* Реформатирование строк в STG */
	proc sql noprint;
		SELECT
			name,
			length
		INTO
			:lmvChrClmnList separated by ' ',
			:lmvChrClmnLengthList separated by ' '
		FROM sashelp.vcolumn
		WHERE libname='ETL_STG' AND memname="STG_&lmvResourceNm." AND type = 'char';
	quit;
	
	%let lmvChrListLength = %sysfunc(countw(&lmvChrClmnList., %str( )));
	
	%if &lmvChrListLength. gt 0 %then %do;
		proc sql noprint;
			connect using etl_stg;
			execute by etl_stg(
				ALTER TABLE ETL_STG.STG_&lmvResourceNm.
					%do i=1 %to &lmvChrListLength.;
						%let lmvClmnName = %scan(&lmvChrClmnList., &i., %str( ));
						%let lmvClmnLength = %scan(&lmvChrClmnLengthList., &i., %str( ));
						
						ALTER COLUMN &lmvClmnName. SET DATA TYPE character varying(%eval(&lmvClmnLength. / 4))
						%if &i. eq &lmvChrListLength. %then %do;
							%str(;)
						%end;
						%else %do;
							%str(,)
						%end;
					%end;
			);
			disconnect from etl_stg;
		quit;
	%end;
	
	%if &SYSCC gt 4 %then %do;
		/* Return session in execution mode */
		OPTIONS NOSYNTAXCHECK OBS=MAX;
		%put ERROR: &lmvResource. was created unsuccessfully: %SYSFUNC(COMPRESS(%SYSFUNC(TRANWRD(ERROR: %NRQUOTE(&SYSERRORTEXT.), %STR(,), %STR(:))), %STR(''"")))!;
		%abort;
	%end;
	
	/* Реформатирование числовых данных в STG */
	proc sql noprint;
		SELECT name INTO :lmvNumericClmnList separated by ' '
		FROM sashelp.vcolumn
		WHERE
			libname='ETL_STG'
			AND memname="STG_&lmvResourceNm."
			AND type = 'num'
			AND format not like 'DATE%';
	quit;
	
	%let lmvNumericListLength = %sysfunc(countw(&lmvNumericClmnList., %str( )));
	
	%if &lmvNumericListLength. gt 0 %then %do;
		proc sql noprint;
			connect using etl_stg;
			execute by etl_stg(
				ALTER TABLE ETL_STG.STG_&lmvResourceNm.
					%do i=1 %to &lmvNumericListLength.;
						%let lmvClmnName = %scan(&lmvNumericClmnList., &i., %str( ));
						
						ALTER COLUMN &lmvClmnName. SET DATA TYPE numeric
						%if &i. eq &lmvNumericListLength. %then %do;
							%str(;)
						%end;
						%else %do;
							%str(,)
						%end;
					%end;
			);
			disconnect from etl_stg;
		quit;
	%end;
	
	%if &SYSCC gt 4 %then %do;
		/* Return session in execution mode */
		OPTIONS NOSYNTAXCHECK OBS=MAX;
		%put ERROR: &lmvResource. was created unsuccessfully: %SYSFUNC(COMPRESS(%SYSFUNC(TRANWRD(ERROR: %NRQUOTE(&SYSERRORTEXT.), %STR(,), %STR(:))), %STR(''"")))!;
		%abort;
	%end;
	
	/* Реформатирование временных данных в STG*/
	proc sql noprint;
		SELECT name INTO :lmvDtList separated by ' '
		FROM sashelp.vcolumn
		WHERE
			libname='ETL_STG'
			AND memname="STG_&lmvResourceNm."
			AND format like 'DATETIME%';
	quit;
	
	%let lmvDtListLength = %sysfunc(countw(&lmvDtList., %str( )));
	
	%if &lmvDtListLength. gt 0 %then %do;
		proc sql noprint;
			connect using etl_stg;
			execute by etl_stg(
				ALTER TABLE ETL_STG.STG_&lmvResourceNm.
					%do i=1 %to &lmvDtListLength.;
						%let lmvClmnName = %scan(&lmvDtList., &i., %str( ));
						
						ALTER COLUMN &lmvClmnName. SET DATA TYPE date
						%if &i. eq &lmvDtListLength. %then %do;
							%str(;)
						%end;
						%else %do;
							%str(,)
						%end;
					%end;
			);
			disconnect from etl_stg;
		quit;
	%end;
	
	%if &SYSCC gt 4 %then %do;
		/* Return session in execution mode */
		OPTIONS NOSYNTAXCHECK OBS=MAX;
		%put ERROR: &lmvResource. was created unsuccessfully: %SYSFUNC(COMPRESS(%SYSFUNC(TRANWRD(ERROR: %NRQUOTE(&SYSERRORTEXT.), %STR(,), %STR(:))), %STR(''"")))!;
		%abort;
	%end;
	
	proc sql noprint;
		connect using etl_stg;
		execute by etl_stg (
			ALTER TABLE ETL_STG.STG_&lmvResourceNm.
			ADD COLUMN etl_extract_id NUMERIC;
		);
		disconnect from etl_stg;
	quit;
	
	%if &SYSCC gt 4 %then %do;
		/* Return session in execution mode */
		OPTIONS NOSYNTAXCHECK OBS=MAX;
		%put ERROR: &lmvResource. was created unsuccessfully: %SYSFUNC(COMPRESS(%SYSFUNC(TRANWRD(ERROR: %NRQUOTE(&SYSERRORTEXT.), %STR(,), %STR(:))), %STR(''"")))!;
		%abort;
	%end;
	
	/* Создание таблиц в ETL_IA: ресурс, дельта, снапшот, снуп */
	%let lmvPkListComma = %sysfunc(tranwrd(&lmvPkList., %str( ), %str(,)));
	
	proc sql noprint;
		/* Ресурс */
		CREATE TABLE ETL_IA.&lmvResourceNm.
		LIKE IA.IA_&lmvResourceNm.;
	quit;
	
	/* Реформатирование строковых данных в ETL_IA*/
	%if &lmvChrListLength. gt 0 %then %do;
		proc sql noprint;
			connect using etl_ia;
			execute by etl_ia(
				ALTER TABLE ETL_IA.&lmvResourceNm.
					%do i=1 %to &lmvChrListLength.;
						%let lmvClmnName = %scan(&lmvChrClmnList., &i., %str( ));
						%let lmvClmnLength = %scan(&lmvChrClmnLengthList., &i., %str( ));
						
						ALTER COLUMN &lmvClmnName. SET DATA TYPE character varying(%eval(&lmvClmnLength. / 4))
						%if &i. eq &lmvChrListLength. %then %do;
							%str(;)
						%end;
						%else %do;
							%str(,)
						%end;
					%end;
			);
			disconnect from etl_ia;
		quit;
	%end;
	
	/* Реформатирование числовых данных в ETL_IA */
	%if &lmvNumericListLength. gt 0 %then %do;
		proc sql noprint;
			connect using etl_ia;
			execute by etl_ia(
				ALTER TABLE ETL_IA.&lmvResourceNm.
					%do i=1 %to &lmvNumericListLength.;
						%let lmvClmnName = %scan(&lmvNumericClmnList., &i., %str( ));
						
						ALTER COLUMN &lmvClmnName. SET DATA TYPE numeric
						%if &i. eq &lmvNumericListLength. %then %do;
							%str(;)
						%end;
						%else %do;
							%str(,)
						%end;
					%end;
			);
			disconnect from etl_ia;
		quit;
	%end;
	
	
	/* Реформатирование временных данных в ETL_IA */
	%if &lmvDtListLength. gt 0 %then %do;
		proc sql noprint;
			connect using etl_ia;
			execute by etl_ia(
				ALTER TABLE ETL_IA.&lmvResourceNm.
					%do i=1 %to &lmvDtListLength.;
						%let lmvClmnName = %scan(&lmvDtList., &i., %str( ));
						
						ALTER COLUMN &lmvClmnName. SET DATA TYPE date
						%if &i. eq &lmvDtListLength. %then %do;
							%str(;)
						%end;
						%else %do;
							%str(,)
						%end;
					%end;
			);
			disconnect from etl_ia;
		quit;
	%end;
	
	proc sql noprint;
		connect using etl_ia;
		execute by etl_ia (
			ALTER TABLE ETL_IA.&lmvResourceNm.
			ADD COLUMN valid_from_dttm timestamp without time zone NOT NULL,
			ADD COLUMN valid_to_dttm timestamp without time zone,
			ADD CONSTRAINT pk_&lmvResourceNm. PRIMARY KEY (&lmvPkListComma.,valid_from_dttm);
			
			/* Дельта */
			CREATE TABLE ETL_IA.&lmvResourceNm._delta (
				LIKE ETL_IA.&lmvResourceNm.,
				etl_delta_cd NUMERIC,
				CONSTRAINT pk_&lmvResourceNm._delta PRIMARY KEY (&lmvPkListComma.,valid_from_dttm,etl_delta_cd)
			);
			
			/* Снапшот */
			CREATE TABLE ETL_IA.&lmvResourceNm._snap (
				LIKE ETL_IA.&lmvResourceNm. ,
				etl_digest1_cd bytea,
				etl_digest2_cd bytea,
				CONSTRAINT pk_&lmvResourceNm._snap PRIMARY KEY (&lmvPkListComma.,valid_from_dttm)
			);
			
			/* Снуп */
			CREATE TABLE ETL_IA.&lmvResourceNm._snup (
				LIKE ETL_IA.&lmvResourceNm._snap,
				etl_delta_cd NUMERIC,
				CONSTRAINT pk_&lmvResourceNm._snup PRIMARY KEY (&lmvPkListComma.,valid_from_dttm,etl_delta_cd)
			);
		);
		disconnect from etl_ia; 
	quit;
	
%mend fmk_add_resource_type;