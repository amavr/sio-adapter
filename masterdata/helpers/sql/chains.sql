SELECT C.KODP, d.KOD_DOG, o.KOD_NUMOBJ, p.KOD_POINT, p.KOD_ATTPOINT, u.KOD_POINT_PU
FROM KR_PAYER c, KR_DOGOVOR d, KR_NUMOBJ o, HR_POINT p, HR_POINT_PU u
WHERE 1=1
--- связи таблиц
AND d.KODP = c.KODP
AND o.KOD_DOG = d.KOD_DOG
AND p.KOD_OBJ = o.KOD_OBJ
AND u.KOD_POINT = p.KOD_POINT
--- доп.условия 
AND d.PR_ACTIVE != 1 -- не архивный
AND o.PR_ACTIVE != 1 -- не архивный
AND o.KOD_PARENT_SA IS NULL -- не субабонент
--- начало цепочки (потребитель)
AND c.INN = '##INN##'
--- конец цепочки (счетчик)
AND u.NOM_PU = '##NOM_PU##'
ORDER BY u.DAT_S DESC