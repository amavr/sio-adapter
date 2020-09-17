WITH last_err(id) AS (
    SELECT MAX(id) FROM SIO_MDM_LOG WHERE ID BETWEEN ##beg_id## AND nvl(##end_id##, id) AND LVL = 'ERR' AND MSG LIKE '##msg##%'
)
,last_tran(ID) AS (
    SELECT tran_id FROM sio_mdm_log l, last_err e WHERE l.id = e.id
)
SELECT m.* FROM SIO_MDM_LOG m, last_tran t WHERE m.TRAN_ID = t.ID ORDER BY m.ID