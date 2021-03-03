%macro tech_get_passw(mpOutPassword=);
	%local lmvHash;
	data _null_;
		infile '~/.authinfo' lrecl=4000;
		input;
		call symputx('lmvHash',scan(_infile_,-1));
	run;
	
	proc groovy;
		add classpath="/opt/sas/spre/home/SASFoundation/lib/base/basejars/default/jars/sas.core.jar";
		eval "import com.sas.util.SasPasswordString;exports.&mpOutPassword=SasPasswordString.decode(""&lmvHash"")";
	run;
%mend tech_get_passw;