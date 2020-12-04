'use strict';

const Utils = require('../helpers/utils');
const Producer = require('../framework/producer');

module.exports = class FakeClient extends Producer {

    constructor(cfg) {
        super(cfg);
        this.delay = cfg.delay;
    }

    startInfo(){
        return `READY with delay = ${this.delay} msec`;
    }

    async handle() {
        await Utils.sleep(this.delay);
        return { id: 123, code: 200, data: '{"message":"I`m from fake producer"}' };
    }

}