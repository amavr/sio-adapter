'use strict';

const log = require('log4js').getLogger('handler.test');

const Consumer = require('../framework/consumer');
const Utils = require('../helpers/utils');
const FileHelper = require('../helpers/file_helper');

module.exports = class TestConsumer extends Consumer {

    constructor(cfg) {
        super(cfg);

        this.delay = cfg.delay;
    }

    init(){
        return new Promise((resolve, reject) => {
            super.init();
            log.info(`READY with delay ${this.delay} msec`);
            resolve();
        });
    }

    async processMsg(msg) {
        await Utils.sleep(this.delay);
    }
}