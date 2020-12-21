'use strict';

const path = require('path');

const FileHelper = require('../helpers/file_helper');
const Producer = require('../framework/producer');

module.exports = class FileClient extends Producer {

    constructor(cfg) {
        super(cfg);
        this.watch_dir = path.join(cfg.work_dir, cfg.watch_dir);
        FileHelper.checkDir(this.watch_dir);
        this.backup_dir = cfg.backup_dir ? path.join(cfg.work_dir, cfg.backup_dir) : null;
        if(this.backup_dir){
            FileHelper.checkDir(this.backup_dir);
        }

        this.buffer = [];
    }

    startInfo(){
        return `WATCH ${this.watch_dir}`;
    }

    async handle() {
        let fname = null;
        try {
            fname = await this.getFile();
            if (fname === undefined) {
                await this.onIdle();
                return null;
            }
            else {
                const sour_path = path.join(this.watch_dir, fname);
                this.info(fname);
                const txt = await FileHelper.read(sour_path);
                if(this.backup_dir){
                    const dest_path = path.join(this.backup_dir, fname);
                    await FileHelper.moveFile(sour_path, dest_path);
                }
                else{
                    await FileHelper.deleteFile(sour_path);
                }
                return { id: fname, code: 200, data: txt };
            }
        }
        catch(ex){
            this.error(ex.message);
            return { id: fname, code: 500, data: ex.message };
        }
    }

    async getFile() {
        const fname = this.getFileFromBuffer();
        if (fname) {
            return fname;
        }
        else {
            await this.refreshBuffer();
            return this.getFileFromBuffer();
        }
    }

    getFileFromBuffer() {
        return this.buffer.shift();
    }

    async refreshBuffer() {
        this.buffer = await FileHelper.getFiles(this.watch_dir);
    }

}