'use strict';

const path = require('path');
const log = require('log4js').getLogger('handler.rec');

const CONST = require('../resources/const.json');
const FileHelper = require('../helpers/file_helper');

module.exports = class MessageRecorder {

    /**
     * 
     * @param {*} cfg
     */
    constructor(cfg) {
        this.err_dir = cfg.err_dir;
        this.msg_dir = cfg.msg_dir;
        this.active = cfg.options.saveMessage;
        this.active = true;
        log.info('READY')
    }

    async onEvent(eventName, sender, data){
        console.log(`RECORDER\t${eventName}`);
    }

    async onMessage(pack) {
        if (this.active) {
            await this.saveFile(pack);
        }
    }

    async saveFile(pack) {
        try {
            const msg = JSON.parse(pack.data);
            const fpath = path.join(this.msg_dir, pack.id);
            await FileHelper.saveObj(fpath, msg);
        }
        catch (ex) {
            log.error(ex.message);
            const fpath = path.join(this.msg_dir, pack.id);
            await FileHelper.save(fpath, pack.data);
        }
    }

}