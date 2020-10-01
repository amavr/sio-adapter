'use strict';

const log = require('log4js').getLogger('cli.fake');

const Utils = require('../helpers/utils');
const Producer = require('../framework/producer');

module.exports = class FakeClient extends Producer {

    constructor(cfg/*, msgFactory*/) {
        super(cfg/*, msgFactory*/);
        this.delay = cfg.delay;
        log.info(`READY with delay = ${this.delay} msec`);
    }

    async handle() {
        await Utils.sleep(this.delay);
        return { id: 123, code: 200, data: '{"message":"I`m from fake producer"}' };
    }

}