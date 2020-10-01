'use strict';

const log = require('log4js').getLogger('cli.file');
const path = require('path');

const FileHelper = require('../helpers/file_helper');
const Producer = require('../framework/producer');

module.exports = class FileClient extends Producer {

    constructor(cfg) {
        super(cfg);
        this.watch_dir = path.join(cfg.work_dir, cfg.watch_dir);
        this.backup_dir = path.join(cfg.work_dir, cfg.backup_dir);
        log.info(`READY on (${this.watch_dir})`);

        this.buffer = [];
    }

    async handle() {
        const fname = await this.getFile();
        if (fname === undefined) {
            await this.onIdle();
            return null;
        }
        else {
            const sour_path = path.join(this.watch_dir, fname);
            const dest_path = path.join(this.backup_dir, fname);
            const txt = await FileHelper.read(sour_path);
            FileHelper.moveFileSync(sour_path, dest_path);
            return { id: fname, code: 200, data: txt };
        }
    }

    async getFile(){
        const fname = this.getFileFromBuffer();
        if(fname){
            return fname;
        }
        else{
            await this.refreshBuffer();
            return this.getFileFromBuffer();
        }
    }

    getFileFromBuffer(){
        return this.buffer.shift();
    }

    async refreshBuffer(){
        this.buffer = await FileHelper.getFiles(this.watch_dir);
    }

}