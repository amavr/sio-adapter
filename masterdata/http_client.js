'use strict';

const EventEmitter = require('events');
const got = require('got');
const log = require('log4js').getLogger('cli.http');

const CONST = require('./resources/const.json');
const Utils = require('./helpers/utils');
const hub = require('./event_hub');

module.exports = class HttpClient extends EventEmitter {

    /**
     * 
     * @param {*} cfg
     */
    constructor(cfg) {
        super();

        this.watch_timer = null;
        this.url = cfg.options.url;
        this.timeout = cfg.options.timeout;
        this.forceStop = false;

        this.on(CONST.EVENTS.ON_NEW_MESSAGE, this.onAnswer);
        this.on(CONST.EVENTS.ON_RECEIVER_IDLE, this.onIdle);
        this.on(CONST.EVENTS.ON_RECEIVE_ERR, this.onError);

        log.info(`READY ${this.url}`);
    }

    async onAnswer(data) {
        await hub.sendEvent(CONST.EVENTS.ON_NEW_MESSAGE, this, data);
    }

    async onIdle() {
        await hub.sendEvent(CONST.EVENTS.ON_RECEIVER_IDLE, this, null);
    }

    async onError(data) {
        await hub.sendEvent(CONST.EVENTS.ON_RECEIVE_ERR, this, data);
    }


    async exec() {
        this.forceStop = false;
        const context = this;
        this.watch_timer = setInterval(async () => {
            context.stop();
            log.debug(`BEG polling of ${this.url}`);
            while (true) {
                const answer = await context.request();
                if (answer.code === 200) {
                    this.emit(CONST.EVENTS.ON_NEW_MESSAGE, answer);
                }
                else if(answer.code === 204){
                    this.emit(CONST.EVENTS.ON_RECEIVER_IDLE, answer);
                    /// остановка цикла опроса для перехода в опрос по таймеру
                    break;
                }
                else {
                    this.emit(CONST.EVENTS.ON_RECEIVE_ERR, answer);

                    // await Utils.sleep(2000);
                    if (context.forceStop) {
                        context.forceStop = false;
                        return;
                    }
                    /// остановка цикла опроса для перехода в опрос по таймеру
                    break;
                }
            }
            hub.sendEvent(CONST.EVENTS.ON_RECEIVER_IDLE, this, null);
            log.debug(`END polling for ${context.timeout} msec`);
            context.exec();
        }, this.timeout);
    }

    stop(force) {
        if (this.watch_timer) {
            clearInterval(this.watch_timer);
            this.watch_timer = null;
            this.forceStop = force === undefined ? false : force;
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