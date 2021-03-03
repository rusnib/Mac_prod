/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для подготовки данных в рамках сквозного процесса для оперпрогноза (продукты)
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
*     %rtp001_load_data_product;
*
****************************************************************************
*  21-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro rtp001_load_data_product;
	%tech_cas_session(mpMode = start
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
						
	%tech_log_event(mpMode=START, mpProcess_Nm=rtp_1_load_data_product);
	%tech_update_resource_status(mpStatus=P, mpResource=rtp_load_data);
	/* 1. загрузка данных в CAS */
	
	%rtp_1_load_data_product(mpMode=A,
					 mpOutTrain=mn_short.all_ml_train,
					 mpOutScore=mn_short.all_ml_scoring,
					 mpWorkCaslib=mn_short);
	
	%tech_update_resource_status(mpStatus=L, mpResource=rtp_load_data);
	%tech_open_resource(mpResource=rtp_abt_pmix);
	
	%tech_log_event(mpMode=START, mpProcess_Nm=rtp_1_load_data_product);
	*%tech_cas_session(mpMode = end
						,mpCasSessNm = casauto
						,mpAssignFlg= y
						,mpAuthinfoUsr=&SYSUSERID.
						);
%mend rtp001_load_data_product;