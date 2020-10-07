'use strict';

const express = require('express');
const path = require('path');
const log4js = require('log4js');
const sqlFormatter = require('sql-formatter');

const cfg = require('../../config').api;
const holder = require('../../masterdata/helpers/sql_holder');
const DBHelper = require('../../masterdata/helpers/db_helper');
const FileHelper = require('../../masterdata/helpers/file_helper');

const log = log4js.getLogger("api");

const http_codes = [204, 301, 304, 400, 401, 403, 404, 500, 503];

const db_helper = new DBHelper(cfg.db);
db_helper.init();

const base_dir = 'D:/IE/files';
const dirs = ['out_message6_1_BYT_MAY_126.files', 'out_message13_1_BYT_MAY_111.files', 'out_message16_1_MAY_125.files'];



const router = express.Router();

router.get('/v1/alive', async (req, res) => {
    res.json({ msg: 'I`m alive' });
    return;
});

router.get('/v1/rnd', async (req, res) => {
    if (process.env.NODE_ENV === 'production') {
        res.json({ msg: 'I`m alive' });
        return;
    }

    const rnd = Math.random() * 100;
    if (rnd < 1) {
        res.status(204).end();
    }
    else {
        const i = Math.floor(Math.random() * dirs.length);
        const dir = path.join(base_dir, dirs[i]);
        const fname = await FileHelper.getRandomFileName(dir);
        const data = await FileHelper.readAsObject(path.join(dir, fname));
        res.header('file', fname);
        res.json(data);
    }
});

router.get('/v1/msg', async (req, res) => {
    if (process.env.NODE_ENV === 'production') {
        res.json({ msg: 'I`m alive' });
        return;
    }

    const dir = 'C:/temp/data/test';

    const n = Math.floor(Math.random() * 11);
    const fname = n < 6 ? 'good-161.json' : n === 8 ? 'bad-161.json' : 'unknown-161.json';
    try {
        const text = await FileHelper.readText(path.join(dir, fname));
        res.header('file', fname);
        if (n < 9) {
            const data = (n < 3) ? text : JSON.parse(text);
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
        log.error(`${fname}\t${ex.message}`);
        res.status(500).json(ex).end();
    }
});

module.exports = router;