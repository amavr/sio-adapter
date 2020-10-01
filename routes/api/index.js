'use strict';

const express = require('express');
const path = require('path');
const log4js = require('log4js');
const sqlFormatter = require('sql-formatter');

const cfg = require('../../config').api;
const holder = require('../../masterdata/helpers/sql_holder');
const DBHelper = require('../../masterdata/helpers/db_helper');

const log = log4js.getLogger();

const http_codes = [204, 301, 304, 400, 401, 403, 404, 500, 503];

const db_helper = new DBHelper(cfg.db);
db_helper.init();

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



module.exports = router;