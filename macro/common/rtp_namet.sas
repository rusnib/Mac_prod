/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Макрос для перекодировки числа дней в название интервала
*
*  ПАРАМЕТРЫ:
*     mpInterval - количество дней
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
*    %day_interval_name(mpInterval=7);
*
****************************************************************************
*  24-07-2020  Борзунов     Начальное кодирование
****************************************************************************/
%macro rtp_namet(mpLint);
	%if &mpLint=7 %then week ;
	%if &mpLint=30 %then month ;
	%if &mpLint=90 %then qtr ;
	%if &mpLint=180 %then halfyear ;
	%if &mpLint=365 %then year ;
%mend rtp_namet;