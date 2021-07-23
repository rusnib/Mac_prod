%macro mCycleAccuracyAnalysis(lmvInputTable = MAX_CASL.GC_FCST_VS_ACT_DEC, lmvKPI = GC, lmvOutTablePostfix = DEC);

	%macro mDummy;
	%mend mDummy;

	cas casauto;
	caslib _all_ assign;
	%let common_path = /opt/sas/mcd_config/macro/step/pt/alerts;
	%include "&common_path./data_prep_pbo.sas"; 
	%data_prep_pbo(
		  mpInLib 		= ETL_IA
		, mpReportDttm 	= &ETL_CURRENT_DTTM.
		, mpOutCasTable = CASUSER.PBO_DICTIONARY
	);
	data WORK.PBO_DICTIONARY;
		set CASUSER.PBO_DICTIONARY;
	run;

	data WORK.INPUT_TABLE;
		set &lmvInputTable.;
	run;
	
	proc sql;
		create table WORK.LIST_0 as 
		select distinct pbo_location_id 
		from WORK.INPUT_TABLE
		;
		create table WORK.LIST as 
		select pbo_location_id, monotonic() as number 
		from WORK.LIST_0
		;
	quit;

	proc sql;
		select count(*) into: mvNumT 
		from WORK.LIST 
		;
	quit;

	proc sql;
	create table WORK.KPI_MONTH( bufsize=65536 )
	  (	
 		  pbo_location_id	num
		, month_dt 			num
		, WAPE_SAS			num
		, WAPE_MCD			num
		, BIAS_SAS			num
		, BIAS_MCD			num
		, sum_gc_act 		num
		, sum_gc_sas_fcst 	num
		, sum_gc_mcd_fcst 	num
		, sum_gc_sas_abserr num
		, sum_gc_mcd_abserr num
		, sum_gc_sas_err 	num
		, sum_gc_mcd_err  	num
	  );
	quit;


	%do i = 1 %to &mvNumT. ;

		proc sql;
			select pbo_location_id into: mvLoc
			from WORK.LIST 
			where number = &i. 
			;
		quit;

		data WORK.INPUT_TABLE_&i.;
			set WORK.INPUT_TABLE;
			where pbo_location_id <> &mvLoc.;
		run;

		proc sql;
			create table WORK.ATOM_PBO_MONTH_&i. as
			select pbo_location_id
				, intnx('month', SALES_DT, 0, 'B') as month_dt
				, sum(gc_act ) as gc_act
		
				, sum(gc_sas_fcst) as gc_sas_fcst
				, sum(gc_mcd_fcst) as gc_mcd_fcst
				
				, (sum(gc_sas_fcst) - sum(gc_act)) as gc_sas_err
				, (sum(gc_mcd_fcst) - sum(gc_act)) as gc_mcd_err
				
				, abs(sum(gc_sas_fcst) - sum(gc_act)) as gc_sas_abserr
				, abs(sum(gc_mcd_fcst) - sum(gc_act)) as gc_mcd_abserr
			from
				WORK.INPUT_TABLE_&i.
			group by 1,2
			;
		quit;
		
		
		/* KPI calculation */
		
		proc sql;
			create table WORK.KPI_MONTH_&i. as
			select month_dt
				, &mvLoc. as pbo_location_id

				, sum(gc_sas_abserr ) / sum(gc_act ) as WAPE_SAS
				, sum(gc_mcd_abserr ) / sum(gc_act ) as WAPE_MCD
		
				, sum(gc_sas_err    ) / sum(gc_act ) as BIAS_SAS
				, sum(gc_mcd_err    ) / sum(gc_act ) as BIAS_MCD
		
				, sum(gc_act ) as sum_gc_act
		
				, sum(gc_sas_fcst) as sum_gc_sas_fcst
				, sum(gc_mcd_fcst) as sum_gc_mcd_fcst
			
				, sum(gc_sas_abserr ) as sum_gc_sas_abserr
				, sum(gc_mcd_abserr ) as sum_gc_mcd_abserr
				
				, sum(gc_sas_err    ) as sum_gc_sas_err
				, sum(gc_mcd_err    ) as sum_gc_mcd_err
				
			from
				WORK.ATOM_PBO_MONTH_&i. 
			group by 1,2
			;
		quit;
		
		data WORK.KPI_MONTH;
			set 
				WORK.KPI_MONTH
				WORK.KPI_MONTH_&i.
			;
		run;

		proc sql;
			drop table			
				  WORK.ATOM_PBO_MONTH_&i. 
				, WORK.INPUT_TABLE_&i.
				, WORK.KPI_MONTH_&i.
			;
		quit;

	%end;

	data WORK.KPI_MONTH_CYCLE;
		set WORK.KPI_MONTH;
		format
			month_dt 			date9.
			WAPE_SAS			PERCENTN8.2
			WAPE_MCD			PERCENTN8.2
			BIAS_SAS			PERCENTN8.2
			BIAS_MCD			PERCENTN8.2
			sum_gc_act 			COMMAX15.
			sum_gc_sas_fcst 	COMMAX15.
			sum_gc_mcd_fcst 	COMMAX15.
			sum_gc_sas_abserr 	COMMAX15.
			sum_gc_mcd_abserr  	COMMAX15.
			sum_gc_sas_err 		COMMAX15.
			sum_gc_mcd_err  	COMMAX15.
		;
	run;

	proc sql;
		create table WORK.KPI_MONTH_CYCLE_&lmvOutTablePostfix. as
		select 
			  main.*
			, loc.*
		from
			WORK.KPI_MONTH_CYCLE as main
		left join
			WORK.PBO_DICTIONARY as loc
		order by
			sum_gc_sas_abserr desc
		;
	quit;



	proc sql;
		drop table 
			, WORK.KPI_MONTH
			, WORK.KPI_MONTH_CYCLE
		;
	quit;







%mend mCycleAccuracyAnalysis;