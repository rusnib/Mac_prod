﻿/*****************************************************************
* НАЗНАЧЕНИЕ:
*   Создает предикат равенства для строки в Oracle.
*
* ПАРАМЕТРЫ:
*   mpText           + строка
*
******************************************************************
* ИСПОЛЬЗУЕТ:
*     %db2_string
*
* УСТАНАВЛИВАЕТ МАКРОПЕРЕМЕННЫЕ:
*     нет
*
******************************************************************
* ПРИМЕР ИСПОЛЬЗОВАНИЯ:
      execute (
         update ...
         where
            job_id %db2_eq_string(&myId)
*
******************************************************************
* 05-07-2012   Нестерёнок  Начальное кодирование
******************************************************************/

%macro db2_eq_string(mpText);
  %if %is_blank(mpText) %then %do;
     IS NULL
  %end;
  %else %do;
     = %db2_string(&mpText)
  %end;
%mend db2_eq_string;