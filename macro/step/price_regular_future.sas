/*****************************************************************
*  ВЕРСИЯ:
*   $Id: 
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*   Реализация алгоритма вычисления регулярных цен на прошлое согласно п.2 постановки McD_price_calculation_v6.
*
*  ПАРАМЕТРЫ:
*	mpPriceRegTable   		 - Наименование входящего справочника НДС, в том числе содержащего регулярные цены от заказчика
*   mpProductAttrTable       - Наименование входящего справочника с атрибутами скю
*	mpOutTable 	  			 - Наименование выходящей таблицы с рассчитанными регулярными ценами на будущее
*
******************************************************************
*  Использует: 
*	нет
*
*  Устанавливает макропеременные:
*   нет
*
******************************************************************
*  Пример использования:
    %price_regular_future(
        mpPriceRegTable   = CASUSER.VAT
        , mpProductAttrTable = CASUSER.PRODUCT_ATTRIBUTES
        , mpOutTable 	  = CASUSER.PRICE_REGULAR_FUTURE
    );
*
****************************************************************************
*  ..-..-2021 		Мугтасимов Данил 		Начальное кодирование
****************************************************************************/
%macro price_regular_future(
    mpPriceRegTable   	    = CASUSER.VAT
    , mpProductAttrTable    = CASUSER.PRODUCT_ATTRIBUTES
    , mpPboLocAttributes	= CASUSER.PBO_LOC_ATTRIBUTES
    , mpPriceIncreaseTable 	= CASUSER.PRICE_INCREASE
    , mpOutTable 	  	    = DM_ABT.PRICE_REGULAR_FUTURE
    );

    %local lmvOutTableName
           lmvOutTableCLib
           lmvRegFutMinDate
           lmvPromoProductIds
        ;

    %let lmvRegFutMinDate = %sysfunc(putn(%sysfunc(inputn(20DEC2020, date9)), yymmdd10));

    /* Обработка таблицы Price Increase */
    data CASUSER.PRICE_INCREASE_MODIFIED_1(drop=L_START_DT);
        set &mpPriceIncreaseTable;
        by PRICE_AREA_NM descending START_DT;
        format L_START_DT END_DT date9.;
        retain L_START_DT;

        L_START_DT = lag(START_DT);

        if first.PRICE_AREA_NM then do;
            END_DT = &ETL_SCD_FUTURE_DT.;
            L_START_DT = .;
            output;
        end;
        else if last.PRICE_AREA_NM then do;
            END_DT = L_START_DT - 1;
            output;
            END_DT = START_DT - 1;
            START_DT = &ETL_SCD_PAST_DT.;
            output;
        end;
        else do;
            END_DT = L_START_DT - 1;
            output;
        end;
    run;

    data CASUSER.PRICE_INCREASE_MODIFIED_2(drop=PERCENT_INCREASE rename=(NEW_PERCENT_INCREASE=PERCENT_INCREASE));
        set CASUSER.PRICE_INCREASE_MODIFIED_1;
        by PRICE_AREA_NM START_DT;
        retain NEW_PERCENT_INCREASE;

        if first.PRICE_AREA_NM then NEW_PERCENT_INCREASE = 1;
        NEW_PERCENT_INCREASE = coalesce(NEW_PERCENT_INCREASE, 1) * coalesce(PERCENT_INCREASE, 1);
    run;

    proc sql noprint;
        select distinct PRODUCT_ID
            into
                :lmvPromoProductIds separated by ","
        from &mpProductAttrTable
        where PRODUCT_ATTR_NM = 'REGULAR_ID'
            and PRODUCT_ID <> input(PRODUCT_ATTR_VALUE, 4.)
            and missing(PRODUCT_ATTR_VALUE) = 0
        ;
    quit;

    %member_names(mpTable=&mpOutTable, mpLibrefNameKey=lmvOutTableCLib, mpMemberNameKey=lmvOutTableName);

    %if %sysfunc(sessfound(casauto))=0 %then %do;
        cas casauto;
        caslib _all_ assign;
    %end;

    proc casutil;  
        droptable casdata="&lmvOutTableName" incaslib="&lmvOutTableCLib" quiet;
    run;

    proc fedsql sessref=casauto;
        create table CASUSER.PRICE_REG_FUT_OUT{options replace=true} as
            select PRODUCT_ID
                , PBO_LOCATION_ID
                , max(START_DT, &lmvRegFutMinDate.) as START_DT
                , END_DT
                , case
                        when missing(PRICE_EAT_IN) = 0 then PRICE_EAT_IN
                        when missing(PRICE_TAKE_OUT) = 0 then PRICE_TAKE_OUT
                        when missing(PRICE_OTHERS) = 0 then PRICE_OTHERS
                        else NULL
                    end as GROSS_PRICE_AMT
                , case
                        when missing(PRICE_EAT_IN) = 0 then divide(PRICE_EAT_IN, (1 + divide(VAT, 100)))
                        when missing(PRICE_TAKE_OUT) = 0 then divide(PRICE_TAKE_OUT, (1 + divide(VAT, 100)))
                        when missing(PRICE_OTHERS) = 0 then divide(PRICE_OTHERS, (1 + divide(VAT, 100)))
                        else NULL
                    end as NET_PRICE_AMT
            from &mpPriceRegTable
            where END_DT >= date %tslit(&lmvRegFutMinDate.)
                and PRODUCT_ID not in (&lmvPromoProductIds.)
        ;
    quit;

    proc fedsql sessref=casauto noprint;
        create table CASUSER.PRICE_REG_FUT_OUT_1{options replace=true} as
            select t1.PRODUCT_ID
                , t1.PBO_LOCATION_ID
                , t1.START_DT as START_DT_PRICE
                , t1.END_DT as END_DT_PRICE
                , t3.START_DT as START_DT_INC
                , t3.END_DT as END_DT_INC
                , coalesce(t2.PBO_LOC_ATTR_VALUE, 'Price Regular') as PBO_LOC_ATTR_VALUE
                , t3.PERCENT_INCREASE
                , case 
                        when (t1.START_DT between t3.START_DT and t3.END_DT) and (t1.END_DT between t3.START_DT and t3.END_DT) then 1
                        when (t3.START_DT < t1.START_DT) and (t3.END_DT between t1.START_DT and t1.END_DT) then 2
                        when (t3.START_DT between t1.START_DT and t1.END_DT) and (t3.END_DT between t1.START_DT and t1.END_DT) then 3
                        when (t3.END_DT > t1.END_DT) and (t3.START_DT between t1.START_DT and t1.END_DT) then 4
                    end as INTERSECT_TYPE
                , t1.NET_PRICE_AMT AS NET_PRICE_AMT_TMP
                , t1.GROSS_PRICE_AMT AS GROSS_PRICE_AMT_TMP
            from CASUSER.PRICE_REG_FUT_OUT t1

            left join ( select PBO_LOCATION_ID
                            , PBO_LOC_ATTR_VALUE
                        from &mpPboLocAttributes
                        where PBO_LOC_ATTR_NM = 'PRICE_AREA_NAME' ) t2
                on t1.PBO_LOCATION_ID = t2.PBO_LOCATION_ID

            left join CASUSER.PRICE_INCREASE_MODIFIED_2 t3
                on coalesce(t2.PBO_LOC_ATTR_VALUE, 'Price Regular') = t3.PRICE_AREA_NM
                    and ( ( (t1.START_DT between t3.START_DT and t3.END_DT) and (t1.END_DT between t3.START_DT and t3.END_DT) )
                    or ( (t3.START_DT < t1.START_DT) and (t3.END_DT between t1.START_DT and t1.END_DT) )
                    or ( (t3.START_DT between t1.START_DT and t1.END_DT) and (t3.END_DT between t1.START_DT and t1.END_DT) )
                    or ( (t3.END_DT > t1.END_DT) and (t3.START_DT between t1.START_DT and t1.END_DT) ) )
        ;
    quit;

    data CASUSER.&lmvOutTableName(keep=PRODUCT_ID PBO_LOCATION_ID START_DT END_DT NET_PRICE_AMT GROSS_PRICE_AMT);
        format START_DT date9. END_DT date9.;
        set CASUSER.PRICE_REG_FUT_OUT_1;
        
        if INTERSECT_TYPE = 1 then do;
            START_DT = START_DT_PRICE;
            END_DT = END_DT_PRICE;
        end;
        if INTERSECT_TYPE = 2 then do;
            START_DT = START_DT_PRICE;
            END_DT = END_DT_INC;
        end;
        if INTERSECT_TYPE = 3 then do;
            START_DT = START_DT_INC;
            END_DT = END_DT_INC;
        end;
        if INTERSECT_TYPE = 4 then do;
            START_DT = START_DT_INC;
            END_DT = END_DT_PRICE;
        end;

        if missing(PERCENT_INCREASE) = 0 and missing(GROSS_PRICE_AMT_TMP) = 0 and missing(NET_PRICE_AMT_TMP) = 0 then do;
            GROSS_PRICE_AMT = round(GROSS_PRICE_AMT_TMP * PERCENT_INCREASE);
            NET_PRICE_AMT = round(NET_PRICE_AMT_TMP * PERCENT_INCREASE, 0.01);
        end;
        else do;
            GROSS_PRICE_AMT = .;
            NET_PRICE_AMT = .;
        end;
    run;

    proc casutil;
        promote casdata="&lmvOutTableName" incaslib="CASUSER" outcaslib="&lmvOutTableCLib";
		save incaslib="&lmvOutTableCLib." outcaslib="&lmvOutTableCLib." casdata="&lmvOutTableName." casout="&lmvOutTableName..sashdat" replace; 
    run;

    proc casutil;
        droptable casdata="PRICE_REG_FUT_OUT" incaslib="CASUSER" quiet;
        droptable casdata="PRICE_INCREASE_MODIFIED_1" incaslib="CASUSER" quiet;
        droptable casdata="PRICE_INCREASE_MODIFIED_2" incaslib="CASUSER" quiet;
        droptable casdata="PRICE_REG_FUT_OUT_1" incaslib="CASUSER" quiet;
    run;

%mend price_regular_future;
