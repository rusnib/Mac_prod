%tech_redirect_log(mpMode=START, mpJobName=price_load_data, mpArea=Manual);
%price_load_data;	
%tech_redirect_log(mpMode=END, mpJobName=price_load_data, mpArea=Manual);


%tech_redirect_log(mpMode=START, mpJobName=price_regular_past, mpArea=Manual);
%price_regular_past(mpPromoTable    	    = CASUSER.PROMO
					, mpPromoPboTable       = CASUSER.PROMO_PBO_UNFOLD
					, mpPromoProdTable      = CASUSER.PROMO_PROD
					, mpProductAttrTable    = CASUSER.PRODUCT_ATTRIBUTES
					, mpPboLocAttributes	= CASUSER.PBO_LOC_ATTRIBUTES
					, mpPriceTable 		    = CASUSER.PRICE
					, mpVatTable 		    = CASUSER.VAT
					, mpPriceIncreaseTable 	= CASUSER.PRICE_INCREASE
					, mpOutTable 		    = MN_DICT.PRICE_REGULAR_PAST
					, mpBatchValue 		    = 50
					);
%tech_redirect_log(mpMode=END, mpJobName=price_regular_past, mpArea=Manual);

%tech_redirect_log(mpMode=START, mpJobName=price_regular_future, mpArea=Manual);
%price_regular_future(mpPriceRegTable   	    = CASUSER.VAT
					, mpProductAttrTable    = CASUSER.PRODUCT_ATTRIBUTES
					, mpPboLocAttributes	= CASUSER.PBO_LOC_ATTRIBUTES
					, mpPriceIncreaseTable 	= CASUSER.PRICE_INCREASE
					, mpOutTable 	  	    = MN_DICT.PRICE_REGULAR_FUTURE
					);
%tech_redirect_log(mpMode=END, mpJobName=price_regular_future, mpArea=Manual);

%tech_redirect_log(mpMode=START, mpJobName=price_promo_past, mpArea=Manual);
%price_promo_past(mpPriceRegPastTab    = MN_DICT.PRICE_REGULAR_PAST
					, mpPromoTable       = CASUSER.PROMO
					, mpPromoPboTable    = CASUSER.PROMO_PBO_UNFOLD
					, mpPromoProdTable   = CASUSER.PROMO_PROD
					, mpProductAttrTable = CASUSER.PRODUCT_ATTRIBUTES
					, mpVatTable 		 = CASUSER.VAT
					, mpOutTable 		 = MN_DICT.PRICE_PROMO_PAST
					, mpBatchValue 		 = 50
					);
%tech_redirect_log(mpMode=END, mpJobName=price_promo_past, mpArea=Manual);

