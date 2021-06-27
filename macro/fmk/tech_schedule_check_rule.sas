%MACRO tech_schedule_check_rule(mpInput=, mpOutput=);

	%local lmvInput
			lmvOutput
			lmvRuleCnt
			lmvBlockCheckList
			lmvRowList
			;
			
	%LET lmvInput=&mpInput.;
	%LET lmvOutput=&mpOutput.;

	DATA &lmvInput.;
			SET &lmvInput.;
			ROW_ID=MONOTONIC();
	RUN;

	PROC SQL NOPRINT;
		SELECT COUNT(*) INTO :lmvRuleCnt SEPARATED BY ""
		FROM &lmvInput.
		;
	QUIT;

	%LET lmvBlockCheckList=;
	%LET lmvRowList=;
	
	%IF &lmvRuleCnt>0 %THEN %DO;
		%DO I=1 %TO &lmvRuleCnt.;
			PROC SQL NOPRINT;
				SELECT STRIP(RULE_COND) as cond, STRIP(RULE_START_HOUR) AS TIME INTO :mvRULE_COND SEPARATED BY "", :mvSTART_TIME SEPARATED BY " "
				FROM &lmvInput
				WHERE ROW_ID=&I.
				;
			QUIT;
			%IF %SYSFUNC(LENGTH("&mvRULE_COND.")) >2 %THEN %DO;
				%IF %SYSFUNC(COUNTW(&mvRULE_COND.,%STR(/)))>1 %THEN %DO;
					%PUT mvCNT_SLASH = %SYSFUNC(COUNTW(&mvRULE_COND.,%STR(/)));
					%LET mvRESOURCE_LIST = %SCAN(%STR(&mvRULE_COND.),1,%STR(/));
					%LET mvRESOURCE_CNT = %SYSFUNC(COUNTW(%STR(&mvRESOURCE_LIST),%STR( )));
					%LET mvSTART_TIME_LENGTH = %LENGTH(&mvSTART_TIME.);
					%PUT &=mvRESOURCE_CNT;

					/* Список ресурсов в формате "таблица_1" "таблица_2" */
					%LET mvRESOURCE_GEN = ;
					%DO J=1 %TO &mvRESOURCE_CNT;
						%LET mvRES_TMP = %SYSFUNC(QUOTE(%SCAN(%STR(&mvRESOURCE_LIST.),&J.,%STR( ))));
						%LET mvRESOURCE_GEN = &mvRESOURCE_GEN. %SYSFUNC(STRIP(&mvRES_TMP.));
					%END;

					%PUT &=mvRESOURCE_GEN;
					
					/* Список статусов */
					%LET mvSTATUS_LIST = %SCAN(%STR(&mvRULE_COND.),-1,%STR(/));
					%LET mvSTATUS_CNT = %SYSFUNC(COUNTW(%STR(&mvSTATUS_LIST),%STR( )));
					
					%LET mvSTATUS_GEN = ;
					%DO J=1 %TO &mvSTATUS_CNT;
						%LET mvSTAT_TMP = %SYSFUNC(QUOTE(%SCAN(%STR(&mvSTATUS_LIST.),&J.,%STR( ))));
						%LET mvSTATUS_GEN = &mvSTATUS_GEN. %SYSFUNC(STRIP(&mvSTAT_TMP.));
					%END;

					%PUT &=mvSTATUS_GEN;
					
					%LET mvBLOCK_CHECK_VALUE=;
					%LET mvNOT_AV_FLAG =;
					PROC SQL NOPRINT;
						SELECT 1 AS CHECK_VARIABLE INTO :mvBLOCK_CHECK_VALUE SEPARATED BY ""
						FROM etl_cfg.cfg_status_table 
						WHERE RESOURCE_NM IN (&mvRESOURCE_GEN.) AND STATUS_CD IN (&mvSTATUS_GEN.)
						%IF &mvSTART_TIME_LENGTH.>0 %THEN %DO;
							AND HOUR(TIME()) >= &mvSTART_TIME.
						%END;
						HAVING COUNT(DISTINCT RESOURCE_NM) = &mvRESOURCE_CNT.
						;
						
						SELECT 1 AS NOT_AVAILABLE_FLAG INTO :mvNOT_AV_FLAG
						FROM etl_cfg.cfg_status_table
						where (resource_nm in (&mvRESOURCE_GEN.) AND status_cd = 'P' )
						;
						
					QUIT;
			
					%IF &mvBLOCK_CHECK_VALUE. = 1 AND &mvNOT_AV_FLAG. =  %THEN %DO;
						%LET lmvBlockCheckList = &lmvBlockCheckList. &I.;	
					%END;
				
				%END;
				/* Блок для RULE_COND формата "today()=today()" */
				%ELSE %DO;
					DATA _NULL_;
						RC=&mvRULE_COND.;
						IF RC THEN DO;
							CALL SYMPUTX("mvRC", 1);
							CALL SYMPUTX("lmvRowList", "&lmvRowList &I.");
						END;
						ELSE DO;
							CALL SYMPUTX("mvRC", 0);
						END;
					RUN;

				%END;
			%END;
			%ELSE %IF %LENGTH("&mvRULE_COND.") = 2 AND %LENGTH(&mvSTART_TIME.)>0 %THEN %DO;
			
				%IF %SYSFUNC(HOUR(%SYSFUNC(TIME()))) >=&mvSTART_TIME. %THEN %DO;
					%LET lmvBlockCheckList = &lmvBlockCheckList. &I.;	
				%END;
			
			%END;
		%END;
	%END;
	%ELSE %DO;
		DATA &lmvOutput.;
			SET &lmvInput.;
			STOP;
		RUN;
	
		%RETURN;
	%END;

	%LET mvROW_ID_LIST_COMMON =&lmvBlockCheckList. &lmvRowList.;
	
	%LET mvSET_COND_GENERAL =(WHERE=(ROW_ID IN (&mvROW_ID_LIST_COMMON.)));
	
	%IF %LENGTH(&mvROW_ID_LIST_COMMON)>0 %THEN %DO;
		DATA &lmvOutput.(DROP=ROW_ID);
			SET &lmvInput. &mvSET_COND_GENERAL.;
		RUN;
	%END;
	%ELSE %DO;
		DATA &lmvOutput.;
			SET &lmvInput.;
			STOP;
		RUN;
	
		%PUT NOTE: NO CONDITION PASSED CHECK;
		%RETURN;
	%END;
%MEND tech_schedule_check_rule;