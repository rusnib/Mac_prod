
/* Импорт прогнозов McDonald's */

/* Путь, где лежат xlsx-файлы */
%let lmvPath = /opt/sas/mcd_config/macro/step/pt/short_term/;

/* Имена xlsx-файлов и по совместительству имена таблиц прогнозов */
%let lmvGC_SALE_DAY 	= MCD_GC_SALES_COUNTRY_DAY;
%let lmvGC_PBO_MONTH 	= MCD_GC_STORE_MONTH;
%let lmvUPT_SKU_MONTH 	= MCD_UPT_SKU_MONTH;


/* Импорт SALE и GC в разрезе дней */
filename reffile1 disk "&lmvPath./&lmvGC_SALE_DAY..xlsx";
proc import 
	datafile	= reffile1
	dbms		= xlsx
	out			= WORK.&lmvGC_SALE_DAY.
	;
	getnames	= yes
	;
run;
proc casutil;
	droptable 
		casdata		= "&lmvGC_SALE_DAY." 
		incaslib	= "MAX_CASL" 
		quiet         
	;                 
run;   
data MAX_CASL.&lmvGC_SALE_DAY.;
	set WORK.&lmvGC_SALE_DAY.;
	/* В файле от McD продажи поделены на 1000 */
	SALES_COMP = 1000 * SALES_COMP;
	rename SALES_D = sales_dt;
run;

/* Импорт GC в разрезе ПБО-месяц */
filename reffile2 disk "&lmvPath./&lmvGC_PBO_MONTH..xlsx";
proc import 
	datafile	= reffile2
	dbms		= xlsx
	out			= WORK.&lmvGC_PBO_MONTH.
	;
	getnames	= yes
	;
run;
proc casutil;
	droptable 
		casdata		= "&lmvGC_PBO_MONTH." 
		incaslib	= "MAX_CASL" 
		quiet         
	;                 
run; 
data MAX_CASL.&lmvGC_PBO_MONTH.;
	set WORK.&lmvGC_PBO_MONTH.;
	rename 
		SALES_M = month_dt	
		PBO		= PBO_LOCATION_ID
	;
run;

/* Импорт UPT в разрезе SKU-месяц */
filename reffile3 disk "&lmvPath./&lmvUPT_SKU_MONTH..xlsx";
proc import 
	datafile	= reffile3
	dbms		= xlsx
	out			= WORK.&lmvUPT_SKU_MONTH.
	;
	getnames	= yes
	;
run;
proc casutil;
	droptable 
		casdata		= "&lmvUPT_SKU_MONTH." 
		incaslib	= "MAX_CASL" 
		quiet         
	;                 
run; 
data MAX_CASL.&lmvUPT_SKU_MONTH. (promote=yes);
	set WORK.&lmvUPT_SKU_MONTH.;
run;


