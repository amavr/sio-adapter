'use strict';

const express = require('express');
const path = require('path');
const log4js = require('log4js');
const sqlFormatter = require('sql-formatter');

const cfg = require('../../config');
const holder = require('../../masterdata/helpers/sql_holder');
const db_helper = require('../../masterdata/helpers/db_helper');
const FileHelper = require('../../masterdata/helpers/file_helper');

const log = log4js.getLogger("api");

const http_codes = [204, 301, 304, 400, 401, 403, 404, 500, 503];

const base_dir = 'D:/IE/files';
const dirs = ['out_message6_1_BYT_MAY_126.files', 'out_message13_1_BYT_MAY_111.files', 'out_message16_1_MAY_125.files'];



const router = express.Router();

router.get('/v1/alive', async (req, res) => {
    res.json({ msg: 'I`m alive' });
    return;
});

router.get('/v1/msg/:msg_id', async (req, res) => {
    const fpath = path.join(cfg.work_dir, cfg.consumers.message_handler.msg_dir, req.params.msg_id);
    try {
        if (FileHelper.FileExistsSync(fpath)) {
            const text = await FileHelper.readText(fpath);
            res.header('file', req.params.msg_id);
            res.json(JSON.parse(text));
        }
        else {
            res.status(404).end();
        }
    }
    catch (ex) {
        log.error(`${fname}\t${ex.message}`);
        res.status(500).json(ex).end();
    }
});

router.delete('/v1/msg/:msg_id', async (req, res) => {
    const fpath = path.join(cfg.work_dir, cfg.consumers.message_handler.msg_dir, req.params.msg_id);
    try {
        if (FileHelper.FileExistsSync(fpath)) {
            const text = await FileHelper.deleteFile(fpath);
            res.header('file', req.params.msg_id);
            res.status(204).end();
        }
        else {
            res.status(404).end();
        }
    }
    catch (ex) {
        log.error(`${fname}\t${ex.message}`);
        res.status(500).json(ex).end();
    }
});

module.exports = router;