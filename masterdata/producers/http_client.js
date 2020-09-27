'use strict';

const got = require('got');
const log = require('log4js').getLogger('cli.http');

const CONST = require('../resources/const.json');
const Utils = require('../helpers/utils');
const hub = require('../framework/event_hub');
const Producer = require('../framework/producer');

module.exports = class HttpClient extends Producer {

    /**
     * 
     * @param {*} cfg
     */
    constructor(cfg) {
        super(cfg);
        this.url = cfg.url;

        log.info(`READY ${this.url}`);
    }

    async handle() {

        /// { id: fname, code: 200, data: txt }
        const answer = await context.request();
        if(answer.code === 204){
            return null;
        }
        else{
            return answer;
        }
    }

    async request() {
        let resp = null;
        const answer = { id: null, data: null, code: 0 }
        try {
            resp = await got(this.url);
        }
        catch (ex) {
            answer.data = ex.message;
            if(ex.response) answer.code = ex.response.statusCode;
            return answer;
        }

        answer.code = resp.statusCode;

        if (answer.code === 200) {
            const msgId = resp.headers['file'];
            // log.info(`${resp.statusCode}\t${resp.headers['content-type']}\t${resp.headers['content-length']}\t${msgId}`);
            if (resp.body.length > 0) {
                answer.id = msgId ? msgId : Utils.getTimeLabel() + '.' + Utils.randomString(3) + '.txt';
                try {
                    answer.data = JSON.parse(resp.body);
                }
                catch (ex) {
                    answer.data = ex.message;
                    answer.code = -200;
                }
            }
            return answer;
        }
        else if (answer.code === 204) {
            // log.warn(`${resp.statusCode} for ${this.url}`);
            return answer;
        }
        else {
            answer.body = `${resp.statusCode} for ${this.url}`;
            // log.warn(answer.body);
            return answer;
        }
    }

}