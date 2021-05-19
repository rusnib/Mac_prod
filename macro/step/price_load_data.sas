%macro price_load_data;
    %local lmvInLib
           lmvReportDt
           lmvReportDttm
        ;
        
    %let lmvInLib=ETL_IA;
    %let lmvReportDt=&ETL_CURRENT_DT.;
    %let lmvReportDttm=&ETL_CURRENT_DTTM.;
    
    %if %sysfunc(sessfound(casauto))=0 %then %do;
        cas casauto;
        caslib _all_ assign;
    %end;
    
     proc casutil;
        droptable casdata="promo" incaslib="casuser" quiet;
        droptable casdata="promo_pbo" incaslib="casuser" quiet;
        droptable casdata="promo_prod" incaslib="casuser" quiet;
        droptable casdata="pbo_dictionary" incaslib="casuser" quiet;
        droptable casdata="PROMO_PBO_UNFOLD" incaslib="casuser" quiet;
        droptable casdata='PRICE' incaslib='casuser' quiet;
        droptable casdata='VAT' incaslib='casuser' quiet;
        droptable casdata='PRODUCT_ATTRIBUTES' incaslib='casuser' quiet;
        droptable casdata='PBO_LOC_ATTRIBUTES' incaslib='casuser' quiet;
        droptable casdata='PRICE_INCREASE' incaslib='casuser' quiet;
        droptable casdata='LBP' incaslib='casuser' quiet;
        droptable casdata='pbo_loc_attr' incaslib='casuser' quiet;
        droptable casdata='PROMO_PBO_ENH_UNFOLD' incaslib='casuser' quiet;
    run;

    /* Подготовка входных данных */
    %add_promotool_marks2(mpOutCaslib=casuser,
                            mpPtCaslib=pt);


    data CASUSER.promo (replace=yes);
        set CASUSER.promo_enh;
    run;
    
    data CASUSER.promo_x_pbo (replace=yes);
        set CASUSER.promo_pbo_enh;
    run;
    
    data CASUSER.promo_x_product (replace=yes);
        set casuser.promo_prod_enh;
    run;

    proc fedsql sessref=casauto noprint;
        create table casuser.promo {options replace=true} as 
        select CHANNEL_CD
        ,PROMO_ID
        ,PROMO_GROUP_ID
        ,PROMO_MECHANICS
        ,PROMO_NM
        ,SEGMENT_ID
        ,PROMO_PRICE_AMT
        ,NP_GIFT_PRICE_AMT
        ,start_dt
        ,end_dt
        from casuser.promo
        where start_dt is not null and end_dt is not null
        ;
    quit;

    proc fedsql sessref=casauto noprint;
        create table casuser.promo_pbo {options replace=true} as 
        select PBO_LOCATION_ID,PROMO_ID
        from casuser.promo_X_PBO
        ;
    quit;

    proc fedsql sessref=casauto noprint;
        create table casuser.promo_prod {options replace=true} as 
        select GIFT_FLAG,OPTION_NUMBER,PRODUCT_ID,PRODUCT_QTY,PROMO_ID, PRICE
        from casuser.promo_X_PRODUCT
        ;
    quit;

    data CASUSER.PRICE (replace=yes drop=valid_from_dttm valid_to_dttm);
        set &lmvInLib..PRICE(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
    run;

/* 	VAT */
    data CASUSER.VAT (replace=yes drop=valid_from_dttm valid_to_dttm);
        set &lmvInLib..vat(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
    run;

/* 	LBP */

    data CASUSER.LBP (replace=yes drop=valid_from_dttm valid_to_dttm);
        set &lmvInLib..LBP(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
    run;

/* PRICE_INCREASE */

    data CASUSER.PRICE_INCREASE (replace=yes drop=valid_from_dttm valid_to_dttm);
        set &lmvInLib..PRICE_INCREASE(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
    run;

/* 	PRODUCT_ATTR */

    data CASUSER.PRODUCT_ATTRIBUTES (replace=yes drop=valid_from_dttm valid_to_dttm);
        set &lmvInLib..PRODUCT_ATTRIBUTES (where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
    run;

    proc fedsql sessref=casauto noprint;
        create table casuser.PRICE{options replace=true} as
        select 
        PRODUCT_ID
        ,PBO_LOCATION_ID
        ,PRICE_TYPE
        ,NET_PRICE_AMT
        ,GROSS_PRICE_AMT
        ,START_DT
        ,END_DT
        from casuser.PRICE
        ;
    quit;
    
    /* PBO_DICTIONARY */
    data CASUSER.PBO_LOCATION (replace=yes drop=valid_from_dttm valid_to_dttm);
        set &lmvInLib..pbo_location(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
    run;
    
    data CASUSER.PBO_LOC_HIERARCHY (replace=yes drop=valid_from_dttm valid_to_dttm);
        set &lmvInLib..PBO_LOC_HIERARCHY(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
    run;

/* PBO_LOC_ATTRIBUTES */

    data CASUSER.PBO_LOC_ATTRIBUTES (replace=yes drop=valid_from_dttm valid_to_dttm);
        set &lmvInLib..PBO_LOC_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
    run;

    proc fedsql sessref=casauto noprint;
       create table casuser.pbo_loc_attr{options replace=true} as
            select distinct *
            from casuser.PBO_LOC_ATTRIBUTES
            ;
    quit;

    proc cas;
    transpose.transpose /
       table={name="pbo_loc_attr", caslib="casuser", groupby={"pbo_location_id"}} 
       attributes={{name="pbo_location_id"}} 
       transpose={"PBO_LOC_ATTR_VALUE"} 
       prefix="A_" 
       id={"PBO_LOC_ATTR_NM"} 
       casout={name="attr_transposed", caslib="casuser", replace=true};
    quit;

    proc fedsql sessref=casauto noprint;
       create table CASUSER.PBO_HIER_FLAT{options replace=true} as
            select t1.pbo_location_id, 
                   t2.PBO_LOCATION_ID as LVL3_ID,
                   t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
                   1 as LVL1_ID
            from 
            (select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=4) as t1
            left join 
            (select * from casuser.PBO_LOC_HIERARCHY where pbo_location_lvl=3) as t2
            on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
            ;
    quit;

    proc fedsql sessref=casauto noprint;
        create table CASUSER.PROMO_PBO_UNFOLD{options replace=true} as
            select t1.PROMO_ID
                   , t2.PBO_LOCATION_ID
            from CASUSER.PROMO_PBO t1 inner join CASUSER.PBO_HIER_FLAT t2
                on t1.pbo_location_id = t2.LVL1_ID
        ;
        create table CASUSER.PBO_EXP2{options replace=true} as
            select t1.PROMO_ID
                   , t2.PBO_LOCATION_ID
            from CASUSER.PROMO_PBO t1 inner join CASUSER.PBO_HIER_FLAT t2
                on t1.pbo_location_id = t2.LVL2_ID
        ;
        create table CASUSER.PBO_EXP3{options replace=true} as
            select t1.PROMO_ID
                   , t2.PBO_LOCATION_ID
            from CASUSER.PROMO_PBO t1 inner join CASUSER.PBO_HIER_FLAT t2
                on t1.pbo_location_id = t2.LVL3_ID
        ;
        create table CASUSER.PBO_EXP4{options replace=true} as
            select t1.PROMO_ID
                   , t2.PBO_LOCATION_ID
            from CASUSER.PROMO_PBO t1 inner join CASUSER.PBO_HIER_FLAT t2
                on t1.pbo_location_id = t2.pbo_location_id
        ;
    quit;

    data CASUSER.PROMO_PBO_UNFOLD(append=force);
        set CASUSER.PBO_EXP2
            CASUSER.PBO_EXP3
            CASUSER.PBO_EXP4;
    run;

    proc fedsql sessref=casauto noprint;
        create table CASUSER.PROMO_PBO_ENH_UNFOLD{options replace=true} as
            select t1.PROMO_ID
                   , t2.PBO_LOCATION_ID
            from CASUSER.PROMO_PBO_ENH t1 inner join CASUSER.PBO_HIER_FLAT t2
                on t1.pbo_location_id = t2.LVL1_ID
        ;
        create table CASUSER.PBO_ENH_EXP2{options replace=true} as
            select t1.PROMO_ID
                   , t2.PBO_LOCATION_ID
            from CASUSER.PROMO_PBO_ENH t1 inner join CASUSER.PBO_HIER_FLAT t2
                on t1.pbo_location_id = t2.LVL2_ID
        ;
        create table CASUSER.PBO_ENH_EXP3{options replace=true} as
            select t1.PROMO_ID
                   , t2.PBO_LOCATION_ID
            from CASUSER.PROMO_PBO_ENH t1 inner join CASUSER.PBO_HIER_FLAT t2
                on t1.pbo_location_id = t2.LVL3_ID
        ;
        create table CASUSER.PBO_ENH_EXP4{options replace=true} as
            select t1.PROMO_ID
                   , t2.PBO_LOCATION_ID
            from CASUSER.PROMO_PBO_ENH t1 inner join CASUSER.PBO_HIER_FLAT t2
                on t1.pbo_location_id = t2.pbo_location_id
        ;
    quit;

    data CASUSER.PROMO_PBO_ENH_UNFOLD(append=force);
        set CASUSER.PBO_ENH_EXP2
            CASUSER.PBO_ENH_EXP3
            CASUSER.PBO_ENH_EXP4;
    run;

    proc fedsql sessref=casauto noprint;
       create table casuser.pbo_dictionary{options replace=true} as
       select t2.pbo_location_id, 
           coalesce(t2.lvl3_id,-999) as lvl3_id,
           coalesce(t2.lvl2_id,-99) as lvl2_id,
           cast(1 as double) as lvl1_id,
           coalesce(t14.pbo_location_nm,'NA') as pbo_location_nm,
           coalesce(t13.pbo_location_nm,'NA') as lvl3_nm,
           coalesce(t12.pbo_location_nm,'NA') as lvl2_nm,
           cast(inputn(t3.A_OPEN_DATE,'ddmmyy10.') as date) as A_OPEN_DATE,
           cast(inputn(t3.A_CLOSE_DATE,'ddmmyy10.') as date) as A_CLOSE_DATE,
           t3.A_PRICE_LEVEL,
           t3.A_DELIVERY,
           t3.A_AGREEMENT_TYPE,
           t3.A_BREAKFAST,
           t3.A_BUILDING_TYPE,
           t3.A_COMPANY,
           t3.A_DRIVE_THRU,
           t3.A_MCCAFE_TYPE,
           t3.A_WINDOW_TYPE
       from casuser.pbo_hier_flat t2
       left join casuser.attr_transposed t3
       on t2.pbo_location_id=t3.pbo_location_id
       left join casuser.pbo_location t14
       on t2.pbo_location_id=t14.pbo_location_id
       left join casuser.pbo_location t13
       on t2.lvl3_id=t13.pbo_location_id
       left join casuser.pbo_location t12
       on t2.lvl2_id=t12.pbo_location_id
       ;
    quit;

    proc casutil;
        promote casdata="pbo_dictionary" incaslib="casuser" outcaslib="casuser";
        promote casdata="PRICE" incaslib="casuser" outcaslib="casuser";
        promote casdata="promo" incaslib="casuser" outcaslib="casuser";
        promote casdata="promo_prod" incaslib="casuser" outcaslib="casuser";
        promote casdata="promo_pbo" incaslib="casuser" outcaslib="casuser";
        promote casdata="VAT" incaslib="casuser" outcaslib="casuser";
        promote casdata="PROMO_PBO_UNFOLD" incaslib="casuser" outcaslib="casuser";
        promote casdata="PRODUCT_ATTRIBUTES" incaslib="casuser" outcaslib="casuser";
        promote casdata="PBO_LOC_ATTRIBUTES" incaslib="casuser" outcaslib="casuser";
        promote casdata="PRICE_INCREASE" incaslib="casuser" outcaslib="casuser";
        promote casdata="LBP" incaslib="casuser" outcaslib="casuser";
        promote casdata="PROMO_PBO_ENH_UNFOLD" incaslib="casuser" outcaslib="casuser";
        droptable casdata='pbo_loc_attr' incaslib='casuser' quiet;
        droptable casdata='pbo_location' incaslib='casuser' quiet;
        droptable casdata='PBO_LOC_HIERARCHY' incaslib='casuser' quiet;
        droptable casdata='pbo_hier_flat' incaslib='casuser' quiet;
        droptable casdata='attr_transposed' incaslib='casuser' quiet;
        droptable casdata='PBO_EXP2' incaslib='casuser' quiet;
        droptable casdata='PBO_EXP3' incaslib='casuser' quiet;
        droptable casdata='PBO_EXP4' incaslib='casuser' quiet;
        droptable casdata='PBO_ENH_EXP2' incaslib='casuser' quiet;
        droptable casdata='PBO_ENH_EXP3' incaslib='casuser' quiet;
        droptable casdata='PBO_ENH_EXP4' incaslib='casuser' quiet;
    quit;

%mend price_load_data;

/*%price_load_data();*/