%macro tech_update_res_status_cycle(mpResource=, mpStatus=);
	%local lmvResource
			lmvStatus
			lmvRuleCond
	;	
	
	%let lmvResource = %sysfunc(upcase(&mpResource.));
	%let lmvStatus = %sysfunc(upcase(&mpStatus.));
	
	/* Поиск правил для данного ресурса*/
	PROC SQL NOPRINT;
		SELECT rule_cond INTO :lmvRuleCond
		FROM etl_cfg.cfg_schedule_rule
		WHERE UPCASE(rule_nm) = UPCASE("&lmvResource.");
	QUIT;
	
	/* Если правило найдено */
	%if %length(&lmvRuleCond.) gt 0 %then %do;
		%let listRuleCond = %scan(&lmvRuleCond., 1, %str(/));
		%let statusRuleCond = %scan(&lmvRuleCond., 2, %str(/));
		
		%if &statusRuleCond. eq A %then %do;
			%let countRuleCond = %sysfunc(countw(&listRuleCond., %str( )));
			
			%do i=1 %to &countRuleCond.;
				%let scanWRuleCond = %scan(&listRuleCond., &i, %str( ));				
				%tech_update_resource_status(mpStatus=&lmvStatus., mpResource=&scanWRuleCond.);
			%end;
		%end;
		%else %do;
			%put WARNING: Rule &lmvResource. has status rule condition &statusRuleCond., not A;
			%return;
		%end;
	%end;
	/* Если не найдено правило - предупреждение и выход */
	%else %do;
		 %put WARNING: Rule for resource &lmvResource. not found in table cfg_schedule_rule;
		 %return;
	%end;
	
%mend tech_update_res_status_cycle	;