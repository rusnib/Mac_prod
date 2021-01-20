﻿/*****************************************************************
* ВЕРСИЯ:
*     $Id: f0c935b3bbecdcbfa01f8173ac8b35d8103be5c2 $
*
******************************************************************
* НАЗНАЧЕНИЕ:
*     Возвращает глубину рекурсии макроса, вызывающего этот,
*     т.е. кол-во присутствий того макроса в стеке вызовов.
*     Таким образом, при вызове из макроса результат всегда будет >= 1.
*     При вызове из глобального режима возвращает 0.
*
* ПАРАМЕТРЫ:
*     нет
*
******************************************************************
* ИСПОЛЬЗУЕТ:
*     нет
*
* УСТАНАВЛИВАЕТ МАКРОПЕРЕМЕННЫЕ:
*     нет
*
******************************************************************
* ПРИМЕР ИСПОЛЬЗОВАНИЯ:
*    %macro mymacro;
*       %if %util_recursion gt 2 %then %return;
*       ...
*       %mymacro;
*       ...
*    %mend;
*
******************************************************************
* 31-08-2012   Нестерёнок  Начальное кодирование
******************************************************************/

%macro util_recursion;
   /* Проверяем вызов из глобального режима */
   %local lmvDepth lmvRecursionCount;
   %let lmvDepth = %eval(%sysmexecdepth - 1);
   %let lmvRecursionCount = 0;

   /* Считаем кол-во вызовов */
   %if &lmvDepth gt 0 %then %do;
      %local lmvCallerName lmvI;
      %let lmvCallerName = %sysmexecname(&lmvDepth);
      %do lmvI=1 %to &lmvDepth;
         %if %sysmexecname(&lmvI) = &lmvCallerName %then
            %let lmvRecursionCount = %eval(&lmvRecursionCount + 1);
      %end;
   %end;

   %do;
      &lmvRecursionCount
   %end;
%mend util_recursion;
