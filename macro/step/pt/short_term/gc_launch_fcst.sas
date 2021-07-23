cas casauto;
caslib _all_ assign;


proc casutil;
	droptable 
		casdata		= "DM_TRAIN_TRP_GC" 
		incaslib	= "MN_DICT" 
		quiet         
	;                 
run;                  

data MN_DICT.DM_TRAIN_TRP_GC;
/* 	set MAX_CASL.DM_TRAIN_TRP_GC_DEC; */
/* 	set MAX_CASL.DM_TRAIN_TRP_GC_JAN; */
/* 	set MAX_CASL.DM_TRAIN_TRP_GC_JAN_2; */
	set MAX_CASL.DM_TRAIN_TRP_GC_MAR;
run;
                      
proc casutil;         
	promote           
		casdata		= "DM_TRAIN_TRP_GC" 
		incaslib	= "MN_DICT" 
		casout		= "DM_TRAIN_TRP_GC"  
		outcaslib	= "MN_DICT"
	;                 
run; 

/* Дата начала прогнозирования и текущая дата и дата начала скоринговой выборки:
'01dec2020'd
'01jan2021'd
'27feb2021'd
*/

%let ETL_CURRENT_DT      =  '27feb2021'd;

%fcst_restore_seasonality(
		  mpInputTbl	= MAX_CASL.TRAIN_ABT_TRP_GC_MAR
		, mpMode 		= GC									
		, mpOutTableNm 	= MAX_CASL.GC_FORECAST_RESTORED_MAR_H90
		, mpAuth 		= NO
	);




/* ***************************************************************************** */
/* ***************************************************************************** */
/* ***************************************************************************** */
/* ***************************************************************************** */
/* ***************************************************************************** */
/* ***************************************************************************** */

cas casauto;
caslib _all_ assign;


proc casutil;
	droptable 
		casdata		= "DM_TRAIN_TRP_PBO" 
		incaslib	= "MN_DICT" 
		quiet         
	;                 
run;                  

data MN_DICT.DM_TRAIN_TRP_PBO;
/* 	set MAX_CASL.DM_TRAIN_TRP_PBO_DEC; */
/* 	set MAX_CASL.DM_TRAIN_TRP_PBO_JAN; */
/* 	set MAX_CASL.DM_TRAIN_TRP_PBO_JAN_2; */
	set MAX_CASL.DM_TRAIN_TRP_PBO_MAR;
run;
                      
proc casutil;         
	promote           
		casdata		= "DM_TRAIN_TRP_PBO" 
		incaslib	= "MN_DICT" 
		casout		= "DM_TRAIN_TRP_PBO"  
		outcaslib	= "MN_DICT"
	;                 
run; 

/* Дата начала прогнозирования и текущая дата и дата начала скоринговой выборки:
'01dec2020'd
'26dec2020'd
'01jan2021'd
'27feb2021'd
*/

%let ETL_CURRENT_DT      =  '27feb2021'd;

%fcst_restore_seasonality(
		  mpInputTbl	= MAX_CASL.TRAIN_ABT_TRP_PBO_MAR
		, mpMode 		= PBO	
		, mpOutTableNm 	= MAX_CASL.PBO_FORECAST_RESTORED_MAR
		, mpAuth 		= NO
	);