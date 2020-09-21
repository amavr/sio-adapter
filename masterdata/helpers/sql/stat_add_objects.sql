WITH x (num, obj, ymd) AS (
SELECT COUNT(1) num, 'ABON', trunc(D_M) FROM KR_OBJECT WHERE U_M = 'I_USER3' GROUP BY trunc(D_M) 
UNION ALL
SELECT COUNT(1) num, 'DOG', trunc(D_M) FROM KR_DOGOVOR WHERE U_M = 'I_USER3' GROUP BY trunc(D_M) 
UNION ALL
SELECT COUNT(1) num, 'OBJ', trunc(D_M) FROM KR_OBJECT WHERE U_M = 'I_USER3' GROUP BY trunc(D_M) 
UNION ALL
SELECT COUNT(1) num, 'ATTP', trunc(D_CREATE) FROM HR_ATTPOINT WHERE d_create < SYSDATE GROUP BY trunc(D_CREATE)
UNION ALL
SELECT COUNT(1) num, 'PU', trunc(D_M) FROM HR_POINT_PU WHERE U_M = 'I_USER3' GROUP BY trunc(D_M)
UNION ALL
SELECT COUNT(1) num, 'IND', trunc(D_M) FROM NR_PRIEM WHERE U_M = 'I_USER3' GROUP BY trunc(D_M)
),
dates(ymd) AS (SELECT DISTINCT ymd FROM x)

SELECT d.ymd, nvl(xc.num, 0) AS ABON, nvl(xd.num, 0) AS DOG, nvl(xo.num, 0) AS OBJ, nvl(xa.num, 0) AS ATTP, nvl(xu.num, 0) AS PU, nvl(xi.num, 0) AS IND
 FROM dates d
 LEFT OUTER JOIN x xc ON (xc.ymd = d.ymd AND xc.obj = 'ABON')
 LEFT OUTER JOIN x xd ON (xd.ymd = d.ymd AND xd.obj = 'DOG')
 LEFT OUTER JOIN x xo ON (xo.ymd = d.ymd AND xo.obj = 'OBJ')
 LEFT OUTER JOIN x xa ON (xa.ymd = d.ymd AND xa.obj = 'ATTP')
 LEFT OUTER JOIN x xu ON (xu.ymd = d.ymd AND xu.obj = 'PU')
 LEFT OUTER JOIN x xi ON (xi.ymd = d.ymd AND xi.obj = 'IND')
WHERE d.ymd BETWEEN TO_DATE('2020-09-07', 'yyyy-mm-dd') AND SYSDATE
ORDER BY d.ymd