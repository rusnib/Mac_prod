/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для обучения моделей MASTERCODE
*	
*
*  ПАРАМЕТРЫ:
*     Нет
*
******************************************************************
*  Использует: 
*	  нет
*
*  Устанавливает макропеременные:
*     нет
*
******************************************************************
*  Пример использования:
*     %rtp008_reconcil;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro rtp008_reconcil;

	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
	
	%tech_log_event(mpMode=START, mpProcess_Nm=rtp_5_reconcil);
	%tech_update_resource_status(mpStatus=P, mpResource=rtp_score_pmix);
	%rtp_5_reconcil(mpFSAbt = mn_short.pbo_train,
							mpMasterCodeTbl = mn_short.MASTER_CODE_DAYS_RESULT,
							mpProductTable = mn_short.PMIX_DAYS_RESULT,
							mpResultTable = mn_short.PMIX_RECONCILED_FULL
							);
	
	%tech_update_resource_status(mpStatus=L, mpResource=rtp_score_pmix);
	%tech_open_resource(mpResource=rtp_reconcil);
	%tech_log_event(mpMode=END, mpProcess_Nm=rtp_5_reconcil);	
%mend rtp008_reconcil;