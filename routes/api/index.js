'use strict';

const express = require('express');
const path = require('path');
const log4js = require('log4js');
const sqlFormatter = require('sql-formatter');

// const cfg = require('../../config');
const holder = require('../../masterdata/helpers/sql_holder');
const db_helper = require('../../masterdata/helpers/db_helper');

const log = log4js.getLogger();

const http_codes = [204, 301, 304, 400, 401, 403, 404, 500, 503];

const router = express.Router();
router.get('/v1/msg', async (req, res) => {
    const dir = 'C:/temp/data/test';
    try {
        const n = Math.floor(Math.random() * 11);
        if (n < 9) {
            const fname = n < 6 ? 'good-161.json'
                : n === 8 ? 'bad-161.json'
                    : 'unknown-161.json';
            const data = await FileHelper.readText(path.join(dir, fname));
            res.header('file', fname);
            res.json(data);
        }
        else if (n === 9) {
            res.status(204).end();
        }
        else {
            const i = Math.floor(Math.random() * 9);
            res.status(http_codes[i]).end();
        }
    }
    catch (ex) {
        log.error(ex);
    }
});

router.get('/v1/times/labels', async (req, res) => {
    try {
        const sql = 'SELECT id, dt, type, name FROM sio_times ORDER BY dt DESC';
        const result = await db_helper.execSql(sql);
        if (result.success) {
            res.send(result.data);
        }
        else {
            res.status(500).send(result.error);
        }
    }
    catch (ex) {
        res.status(500).send(ex.message);
    }
});

router.get('/v1/times/labels/:labelId', async (req, res) => {
    try {
        const sql = `SELECT code, val FROM sio_counters WHERE SIO_TIME_ID = '${req.params.labelId}' ORDER BY code`;
        const result = await db_helper.execSql(sql);
        if (result.success) {
            res.send(result.data);
        }
        else {
            res.status(500).send(result.error);
        }
    }
    catch (ex) {
        res.status(500).send(ex.message);
    }
});



router.post('/v1/sql', async (req, res) => {
    const sql = req.body.sql;
    const fmt = sqlFormatter.format(sql, { language: "pl/sql", indent: '\t' }).replace(/=\s+>/g, '=>');
    // res.setHeader('Content-Type', 'text/plain');
    res.send({ sql: fmt });
});

router.post('/v1/errors', async (req, res) => {
    const cond = req.body;
    const sql = holder.get('stat_log_errors').replace('##beg_id##', cond.beg_id).replace('##end_id##', cond.end_id);
    const result = await db_helper.execSql(sql);
    const data = result.data.map((val, i) => {
        return [val.MSG, val.NUM];
    });
    res.send(data);
});

router.post('/v1/transact', async (req, res) => {
    const cond = req.body;
    const sql = holder.get('tran_rows')
        .replace('##beg_id##', cond.beg_id)
        .replace('##end_id##', cond.end_id)
        .replace('##msg##', cond.msg);

    const result = await db_helper.execSql(sql);
    const data = result.data;
    if (result.success) {
        res.send(data);
    }
    else {
        res.status(500).send(result.error);
    }
});

router.get('/v1/stat/objects/added', async (req, res) => {
    const cond = req.body;
    const sql = holder.get('stat_add_objects');
    const result = await db_helper.execSql(sql);
    const counters = ['', 0, 0, 0, 0, 0, 0];
    const data = [];
    result.data.forEach(val => {
        counters[0] = val.YMD;
        counters[1] += val.ABON;
        counters[2] += val.DOG;
        counters[3] += val.OBJ;
        counters[4] += val.ATTP;
        counters[5] += val.PU;
        counters[6] += val.IND;
        data.push(Array.from(counters));
    });
    // const data = result.data.map((val, i) => {
    //     counters[0] = val.YMD;
    //     counters[1] += val.ABON;
    //     counters[2] += val.DOG;
    //     counters[3] += val.OBJ;
    //     counters[4] += val.ATTP;
    //     counters[5] += val.PU;
    //     counters[6] += val.IND;
    //     return Array.from(counters);//[val.YMD, val.ABON, val.DOG, val.OBJ, val.ATTP, val.PU, val.IND];
    // });
    res.header("Cache-Control", "no-cache, no-store, must-revalidate");
    res.header("Pragma", "no-cache");
    res.header("Expires", 0);
    res.send(data);
});



module.exports = router;