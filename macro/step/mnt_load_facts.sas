/* ********************************************************************* */
/* ********************************************************************* */
/* Джоб для запуска сквозного процесса прогнозирования временными рядами */
/* ********************************************************************* */
/* ********************************************************************* */
%macro mnt_load_facts;
	%tech_redirect_log(mpMode=START, mpJobName=dp_load_facts, mpArea=Main);
			%dp_load_facts(mpFactGcMnth = mn_dict.fact_gc_month,
						mpFactPmixMnth = mn_dict.fact_pmix_month,
						mpFactUptMnth = mn_dict.fact_upt_month,
						mpPath = /data/files/output/dp_files/
						);
	%tech_redirect_log(mpMode=END, mpJobName=dp_load_facts, mpArea=Main);
	
	/*
	%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FaM_KOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_GC_FaM_KOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FaM_KOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_UPT_FaM_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_GC_FaM_NONKOMP);
	%dp_jobexecution(mpJobName=ACT_LOAD_QNT_FaM_NONKOMP);
	*/
%mend mnt_load_facts;
