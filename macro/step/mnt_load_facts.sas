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
%mend mnt_load_facts;
