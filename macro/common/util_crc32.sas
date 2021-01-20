﻿/*****************************************************************
*  ВЕРСИЯ:
*     $Id: 999065513d9d1a55cb91fdb0aeeb3436840024b8 $
*
******************************************************************
*  НАЗНАЧЕНИЕ:
*     Рассчитывает хэш-сумму переданной строки по CRC-32.
*
*  ПАРАМЕТРЫ:
*     mpText                  +  строка, для которой вычисляется хэш-сумма
*
******************************************************************
*  Использует:
*     нет
*
*  Устанавливает макропеременные:
*     нет
*
*  Ограничения:
*     нет
*
******************************************************************
*  Пример использования:
*     %local lmvCRC32;
*     %let lmvCRC32 = %util_crc32 (123456789);
*     // равно CBF43926
*
******************************************************************
*  26-12-2017   Нестерёнок  Начальное кодирование
******************************************************************/

%macro util_crc32 (mpText);
   %local lmvLkupTable;
   /*
      00000000 77073096 ee0e612c 990951ba 076dc419 706af48f e963a535 9e6495a3
      0edb8832 79dcb8a4 e0d5e91e 97d2d988 09b64c2b 7eb17cbd e7b82d07 90bf1d91
      1db71064 6ab020f2 f3b97148 84be41de 1adad47d 6ddde4eb f4d4b551 83d385c7
      136c9856 646ba8c0 fd62f97a 8a65c9ec 14015c4f 63066cd9 fa0f3d63 8d080df5
      3b6e20c8 4c69105e d56041e4 a2677172 3c03e4d1 4b04d447 d20d85fd a50ab56b
      35b5a8fa 42b2986c dbbbc9d6 acbcf940 32d86ce3 45df5c75 dcd60dcf abd13d59
      26d930ac 51de003a c8d75180 bfd06116 21b4f4b5 56b3c423 cfba9599 b8bda50f
      2802b89e 5f058808 c60cd9b2 b10be924 2f6f7c87 58684c11 c1611dab b6662d3d
      76dc4190 01db7106 98d220bc efd5102a 71b18589 06b6b51f 9fbfe4a5 e8b8d433
      7807c9a2 0f00f934 9609a88e e10e9818 7f6a0dbb 086d3d2d 91646c97 e6635c01
      6b6b51f4 1c6c6162 856530d8 f262004e 6c0695ed 1b01a57b 8208f4c1 f50fc457
      65b0d9c6 12b7e950 8bbeb8ea fcb9887c 62dd1ddf 15da2d49 8cd37cf3 fbd44c65
      4db26158 3ab551ce a3bc0074 d4bb30e2 4adfa541 3dd895d7 a4d1c46d d3d6f4fb
      4369e96a 346ed9fc ad678846 da60b8d0 44042d73 33031de5 aa0a4c5f dd0d7cc9
      5005713c 270241aa be0b1010 c90c2086 5768b525 206f85b3 b966d409 ce61e49f
      5edef90e 29d9c998 b0d09822 c7d7a8b4 59b33d17 2eb40d81 b7bd5c3b c0ba6cad
      edb88320 9abfb3b6 03b6e20c 74b1d29a ead54739 9dd277af 04db2615 73dc1683
      e3630b12 94643b84 0d6d6a3e 7a6a5aa8 e40ecf0b 9309ff9d 0a00ae27 7d079eb1
      f00f9344 8708a3d2 1e01f268 6906c2fe f762575d 806567cb 196c3671 6e6b06e7
      fed41b76 89d32be0 10da7a5a 67dd4acc f9b9df6f 8ebeeff9 17b7be43 60b08ed5
      d6d6a3e8 a1d1937e 38d8c2c4 4fdff252 d1bb67f1 a6bc5767 3fb506dd 48b2364b
      d80d2bda af0a1b4c 36034af6 41047a60 df60efc3 a867df55 316e8eef 4669be79
      cb61b38c bc66831a 256fd2a0 5268e236 cc0c7795 bb0b4703 220216b9 5505262f
      c5ba3bbe b2bd0b28 2bb45a92 5cb36a04 c2d7ffa7 b5d0cf31 2cd99e8b 5bdeae1d
      9b64c2b0 ec63f226 756aa39c 026d930a 9c0906a9 eb0e363f 72076785 05005713
      95bf4a82 e2b87a14 7bb12bae 0cb61b38 92d28e9b e5d5be0d 7cdcefb7 0bdbdf21
      86d3d2d4 f1d4e242 68ddb3f8 1fda836e 81be16cd f6b9265b 6fb077e1 18b74777
      88085ae6 ff0f6a70 66063bca 11010b5c 8f659eff f862ae69 616bffd3 166ccf45
      a00ae278 d70dd2ee 4e048354 3903b3c2 a7672661 d06016f7 4969474d 3e6e77db
      aed16a4a d9d65adc 40df0b66 37d83bf0 a9bcae53 debb9ec5 47b2cf7f 30b5ffe9
      bdbdf21c cabac28a 53b39330 24b4a3a6 bad03605 cdd70693 54de5729 23d967bf
      b3667a2e c4614ab8 5d681b02 2a6f2b94 b40bbe37 c30c8ea1 5a05df1b 2d02ef8d
   */
   %let lmvLkupTable =
      0          1996959894 3993919788 2567524794 124634137  1886057615 3915621685 2657392035 249268274  2044508324 3772115230 2547177864 162941995  2125561021 3887607047 2428444049
      498536548  1789927666 4089016648 2227061214 450548861  1843258603 4107580753 2211677639 325883990  1684777152 4251122042 2321926636 335633487  1661365465 4195302755 2366115317
      997073096  1281953886 3579855332 2724688242 1006888145 1258607687 3524101629 2768942443 901097722  1119000684 3686517206 2898065728 853044451  1172266101 3705015759 2882616665
      651767980  1373503546 3369554304 3218104598 565507253  1454621731 3485111705 3099436303 671266974  1594198024 3322730930 2970347812 795835527  1483230225 3244367275 3060149565
      1994146192 31158534   2563907772 4023717930 1907459465 112637215  2680153253 3904427059 2013776290 251722036  2517215374 3775830040 2137656763 141376813  2439277719 3865271297
      1802195444 476864866  2238001368 4066508878 1812370925 453092731  2181625025 4111451223 1706088902 314042704  2344532202 4240017532 1658658271 366619977  2362670323 4224994405
      1303535960 984961486  2747007092 3569037538 1256170817 1037604311 2765210733 3554079995 1131014506 879679996  2909243462 3663771856 1141124467 855842277  2852801631 3708648649
      1342533948 654459306  3188396048 3373015174 1466479909 544179635  3110523913 3462522015 1591671054 702138776  2966460450 3352799412 1504918807 783551873  3082640443 3233442989
      3988292384 2596254646 62317068   1957810842 3939845945 2647816111 81470997   1943803523 3814918930 2489596804 225274430  2053790376 3826175755 2466906013 167816743  2097651377
      4027552580 2265490386 503444072  1762050814 4150417245 2154129355 426522225  1852507879 4275313526 2312317920 282753626  1742555852 4189708143 2394877945 397917763  1622183637
      3604390888 2714866558 953729732  1340076626 3518719985 2797360999 1068828381 1219638859 3624741850 2936675148 906185462  1090812512 3747672003 2825379669 829329135  1181335161
      3412177804 3160834842 628085408  1382605366 3423369109 3138078467 570562233  1426400815 3317316542 2998733608 733239954  1555261956 3268935591 3050360625 752459403  1541320221
      2607071920 3965973030 1969922972 40735498   2617837225 3943577151 1913087877 83908371   2512341634 3803740692 2075208622 213261112  2463272603 3855990285 2094854071 198958881
      2262029012 4057260610 1759359992 534414190  2176718541 4139329115 1873836001 414664567  2282248934 4279200368 1711684554 285281116  2405801727 4167216745 1634467795 376229701
      2685067896 3608007406 1308918612 956543938  2808555105 3495958263 1231636301 1047427035 2932959818 3654703836 1088359270 936918000  2847714899 3736837829 1202900863 817233897
      3183342108 3401237130 1404277552 615818150  3134207493 3453421203 1423857449 601450431  3009837614 3294710456 1567103746 711928724  3020668471 3272380065 1510334235 755167117

   ;
   %local lmv0xff lmv0xffffffff;
   %let lmv0xff = 255;
   %let lmv0xffffffff = 4294967295;

   %local lmvCRC lmvLen lmvI lmvChar lmvJ lmvC;
   %let lmvCRC = &lmv0xffffffff;
   %let lmvLen = %sysfunc(klength(%bquote(&mpText)));

   %local lmvB lmvA1 lmvA2 lmvA3;
   %do lmvI=1 %to &lmvLen;
       %let lmvChar = %sysfunc(ksubstr(%bquote(&mpText), &lmvI, 1), $hex8.);
       %do lmvJ=1 %to %length(&lmvChar) %by 2;
          %let lmvC = %sysfunc(substr(&lmvChar, &lmvJ, 2));
          %let lmvB = %sysfunc(inputn(&lmvC, hex2.));

          /* crc = (crc >>> 8) ^ table[(crc ^ b) & 0xff]; */
          %let lmvA1 = %sysfunc(brshift(&lmvCRC, 8));
          %let lmvA2 = %sysfunc(band(%sysfunc(bxor(&lmvCRC, &lmvB)), &lmv0xff));
          %let lmvA3 = %qscan(&lmvLkupTable, %eval(&lmvA2+1));
          %let lmvCRC = %sysfunc(bxor(&lmvA1, &lmvA3));
      %end;
   %end;

   /* crc = crc ^ 0xffffffff; */
   %let lmvCRC = %sysfunc(bxor(&lmvCRC, &lmv0xffffffff), hex8.);

   %do;&lmvCRC%end;
%mend util_crc32;