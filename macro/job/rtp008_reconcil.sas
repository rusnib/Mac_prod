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

	%let etls_jobName=rtp008_reconcil;
	%etl_job_start;
	
	%rtp_5_reconcil(mpFSAbt = dm_abt.pbo_train,
					mpMasterCodeTbl = dm_abt.MASTER_CODE_DAYS_RESULT,
					mpProductTable = DM_ABT.PMIX_DAYS_RESULT,
					mpResultTable = DM_ABT.PMIX_RECONCILED_FULL
					);

	%etl_job_finish;
	
%mend rtp008_reconcil;