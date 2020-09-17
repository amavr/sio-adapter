'use strict';

const log = require('log4js').getLogger('cli.file');
const path = require('path');

const CONST = require('./resources/const.json');
const Utils = require('./helpers/utils');
const FileHelper = require('./helpers/file_helper');
const hub = require('./event_hub');
const Timex = require('./helpers/timex');

module.exports = class FileClient extends Timex {

    constructor(cfg) {
        super(cfg);
        this.watch_dir = cfg.watch_dir;
        this.backup_dir = cfg.backup_dir;
        this.tag = 'FILES';
        log.info(`READY on (${this.watch_dir})`);
    }

    async handle() {
        let res = false;
        try {
            const files = await FileHelper.getFiles(this.watch_dir);
            if (files.length > 0) {
                const sour_path = path.join(this.watch_dir, files[0]);
                const dest_path = path.join(this.backup_dir, files[0]);
                const txt = await FileHelper.read(sour_path);
                await this.onData({ id: files[0], code: 200, data: txt });
                FileHelper.moveFile(sour_path, dest_path);
                res = true;
            }
            else {
                await this.onIdle();
            }
        }
        catch (ex) {
            await this.onError(ex.message);
        }
        return res;
    }

}