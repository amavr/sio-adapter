'use strict';

const Consumer = require('../framework/consumer');
const Utils = require('../helpers/utils');
const FileHelper = require('../helpers/file_helper');

module.exports = class TestConsumer extends Consumer {

    constructor(cfg) {
        super(cfg);

        this.delay = cfg.delay;
    }

    init(){
        const context = this;
        return new Promise((resolve, reject) => {
            super.init();
            context.log.info(`READY`);
            resolve();
        });
    }

    async processMsg(msg) {
        await Utils.sleep(this.delay);
    }
}