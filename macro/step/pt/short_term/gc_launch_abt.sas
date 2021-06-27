cas casauto;
caslib _all_ assign;

/* Дата начала прогнозирования и текущая дата и дата начала скоринговой выборки:
'01dec2020'd
'01jan2021'd		'26dec2020'd
'27feb2021'd
*/

%let ETL_CURRENT_DT      =  '26dec2020'd;


%fcst_create_abt_pbo_gc(
			  mpMode		  = gc
			, mpOutTableDmVf  = MAX_CASL.DM_TRAIN_TRP_GC_JAN_2
			, mpOutTableDmABT = MAX_CASL.TRAIN_ABT_TRP_GC_JAN_2
		);


%fcst_create_abt_pbo_gc(
			  mpMode		  = pbo
			, mpOutTableDmVf  = MAX_CASL.DM_TRAIN_TRP_PBO_JAN_2
			, mpOutTableDmABT = MAX_CASL.TRAIN_ABT_TRP_PBO_JAN_2
		);


DATA CASUSER.DM_TRAIN_TRP_GC_DEC(replace=yes);
	set MAX_CASL.DM_TRAIN_TRP_GC_DEC(where=(sales_dt>=intnx('year', '01DEC2020'd, - 4, 's')));
	format sales_dt date9.;
RUN;


DATA CASUSER.DM_TRAIN_TRP_PBO_DEC(replace=yes);
	set MAX_CASL.DM_TRAIN_TRP_PBO_DEC(where=(sales_dt>=intnx('year', '01DEC2020'd, - 4, 's')));
	format sales_dt date9.;
RUN;


DATA CASUSER.TRAIN_ABT_TRP_GC_DEC(replace=yes);
	set MAX_CASL.TRAIN_ABT_TRP_GC_DEC(where=(sales_dt>=intnx('year', '01DEC2020'd, - 4, 's')));
	format sales_dt date9.;
RUN;


DATA CASUSER.TRAIN_ABT_TRP_PBO_DEC(replace=yes);
	set MAX_CASL.TRAIN_ABT_TRP_PBO_DEC(where=(sales_dt>=intnx('year', '01DEC2020'd, - 4, 's')));
	format sales_dt date9.;
RUN;

DATA CASUSER.DM_TRAIN_TRP_GC_JAN(replace=yes);
	set MAX_CASL.DM_TRAIN_TRP_GC_JAN(where=(sales_dt>=intnx('year', '01jan2021'd, - 4, 's')));
	format sales_dt date9.;
RUN;


DATA CASUSER.DM_TRAIN_TRP_PBO_JAN(replace=yes);
	set MAX_CASL.DM_TRAIN_TRP_PBO_JAN(where=(sales_dt>=intnx('year', '01jan2021'd, - 4, 's')));
	format sales_dt date9.;
RUN;


DATA CASUSER.TRAIN_ABT_TRP_GC_JAN(replace=yes);
	set MAX_CASL.TRAIN_ABT_TRP_GC_JAN(where=(sales_dt>=intnx('year', '01jan2021'd, - 4, 's')));
	format sales_dt date9.;
RUN;


DATA CASUSER.TRAIN_ABT_TRP_PBO_JAN(replace=yes);
	set MAX_CASL.TRAIN_ABT_TRP_PBO_JAN(where=(sales_dt>=intnx('year', '01jan2021'd, - 4, 's')));
	format sales_dt date9.;
RUN;

proc fedsql sessref=casauto;
/* 	Select min(sales_dt) from MAX_CASL.DM_TRAIN_TRP_GC_DEC    ; */
/* 	Select min(sales_dt) from MAX_CASL.TRAIN_ABT_TRP_GC_DEC   ; */
/* 	Select min(sales_dt) from MAX_CASL.DM_TRAIN_TRP_PBO_DEC   ; */
/* 	Select min(sales_dt) from MAX_CASL.TRAIN_ABT_TRP_PBO_DEC  ; */
/*  */
/* 	Select min(sales_dt) from MAX_CASL.DM_TRAIN_TRP_GC_JAN    ; */
/* 	Select min(sales_dt) from MAX_CASL.TRAIN_ABT_TRP_GC_JAN   ; */
/* 	Select min(sales_dt) from MAX_CASL.DM_TRAIN_TRP_PBO_JAN   ; */
/* 	Select min(sales_dt) from MAX_CASL.TRAIN_ABT_TRP_PBO_JAN  ; */

	Select min(sales_dt) from MAX_CASL.DM_TRAIN_TRP_GC_MAR    ;
	Select min(sales_dt) from MAX_CASL.TRAIN_ABT_TRP_GC_MAR   ;
	Select min(sales_dt) from MAX_CASL.DM_TRAIN_TRP_PBO_MAR   ;
	Select min(sales_dt) from MAX_CASL.TRAIN_ABT_TRP_PBO_MAR  ;
quit;

proc fedsql sessref=casauto;
/* 	Select max(sales_dt) from MAX_CASL.DM_TRAIN_TRP_GC_DEC    ; */
/* 	Select max(sales_dt) from MAX_CASL.TRAIN_ABT_TRP_GC_DEC   ; */
/* 	Select max(sales_dt) from MAX_CASL.DM_TRAIN_TRP_PBO_DEC   ; */
/* 	Select max(sales_dt) from MAX_CASL.TRAIN_ABT_TRP_PBO_DEC  ; */
/*  */
/* 	Select max(sales_dt) from MAX_CASL.DM_TRAIN_TRP_GC_JAN    ; */
/* 	Select max(sales_dt) from MAX_CASL.TRAIN_ABT_TRP_GC_JAN   ; */
/* 	Select max(sales_dt) from MAX_CASL.DM_TRAIN_TRP_PBO_JAN   ; */
/* 	Select max(sales_dt) from MAX_CASL.TRAIN_ABT_TRP_PBO_JAN  ; */

	Select max(sales_dt) from MAX_CASL.DM_TRAIN_TRP_GC_MAR    ;
	Select max(sales_dt) from MAX_CASL.TRAIN_ABT_TRP_GC_MAR   ;
	Select max(sales_dt) from MAX_CASL.DM_TRAIN_TRP_PBO_MAR   ;
	Select max(sales_dt) from MAX_CASL.TRAIN_ABT_TRP_PBO_MAR  ;
quit;


proc casutil;
	droptable casdata = "DM_TRAIN_TRP_GC_DEC" incaslib	= "MAX_CASL"  quiet;
	droptable casdata = "TRAIN_ABT_TRP_GC_DEC" incaslib	= "MAX_CASL"  quiet;	
	droptable casdata = "DM_TRAIN_TRP_PBO_DEC" incaslib	= "MAX_CASL"  quiet;
	droptable casdata = "TRAIN_ABT_TRP_PBO_DEC" incaslib= "MAX_CASL"  quiet;
	
	droptable casdata = "DM_TRAIN_TRP_GC_JAN" incaslib	= "MAX_CASL"  quiet;
	droptable casdata = "TRAIN_ABT_TRP_GC_JAN" incaslib	= "MAX_CASL"  quiet;
	droptable casdata = "DM_TRAIN_TRP_PBO_JAN" incaslib	= "MAX_CASL"  quiet;
	droptable casdata = "TRAIN_ABT_TRP_PBO_JAN" incaslib= "MAX_CASL"  quiet;
	
/* 	droptable casdata = "DM_TRAIN_TRP_GC_MAR" incaslib	= "MAX_CASL"  quiet; */
/* 	droptable casdata = "TRAIN_ABT_TRP_GC_MAR" incaslib	= "MAX_CASL"  quiet; */
/* 	droptable casdata = "DM_TRAIN_TRP_PBO_MAR" incaslib	= "MAX_CASL"  quiet; */
/* 	droptable casdata = "TRAIN_ABT_TRP_PBO_MAR" incaslib= "MAX_CASL"  quiet; */
run; 



proc casutil;         
	promote casdata = "DM_TRAIN_TRP_GC_DEC" 	incaslib = "CASUSER"  casout = "DM_TRAIN_TRP_GC_DEC"  outcaslib = "MAX_CASL";
	promote casdata = "TRAIN_ABT_TRP_GC_DEC" 	incaslib = "CASUSER"  casout = "TRAIN_ABT_TRP_GC_DEC"  outcaslib = "MAX_CASL";
	promote casdata = "DM_TRAIN_TRP_PBO_DEC" 	incaslib = "CASUSER"  casout = "DM_TRAIN_TRP_PBO_DEC"  outcaslib = "MAX_CASL";	
	promote casdata = "TRAIN_ABT_TRP_PBO_DEC" 	incaslib = "CASUSER"  casout = "TRAIN_ABT_TRP_PBO_DEC"  outcaslib = "MAX_CASL";
	                                                        
	promote casdata = "DM_TRAIN_TRP_GC_JAN" 	incaslib = "CASUSER"  casout = "DM_TRAIN_TRP_GC_JAN"  outcaslib = "MAX_CASL";
	promote casdata = "TRAIN_ABT_TRP_GC_JAN" 	incaslib = "CASUSER"  casout = "TRAIN_ABT_TRP_GC_JAN"  outcaslib = "MAX_CASL";
	promote casdata = "DM_TRAIN_TRP_PBO_JAN" 	incaslib = "CASUSER"  casout = "DM_TRAIN_TRP_PBO_JAN"  outcaslib = "MAX_CASL";	
	promote casdata = "TRAIN_ABT_TRP_PBO_JAN" 	incaslib = "CASUSER"  casout = "TRAIN_ABT_TRP_PBO_JAN"  outcaslib = "MAX_CASL";
	                                                        
/* 	promote casdata = "DM_TRAIN_TRP_GC_MAR" 	incaslib = "CASUSER"  casout = "DM_TRAIN_TRP_GC_MAR"  outcaslib = "MAX_CASL"; */
/* 	promote casdata = "TRAIN_ABT_TRP_GC_MAR" 	incaslib = "CASUSER"  casout = "TRAIN_ABT_TRP_GC_MAR"  outcaslib = "MAX_CASL"; */
/* 	promote casdata = "DM_TRAIN_TRP_PBO_MAR" 	incaslib = "CASUSER"  casout = "DM_TRAIN_TRP_PBO_MAR"  outcaslib = "MAX_CASL";	 */
/* 	promote casdata = "TRAIN_ABT_TRP_PBO_MAR" 	incaslib = "CASUSER"  casout = "TRAIN_ABT_TRP_PBO_MAR"  outcaslib = "MAX_CASL"; */
run;  