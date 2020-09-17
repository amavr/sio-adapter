'use strict';

const EventEmitter = require('events');
const got = require('got');
const log = require('log4js').getLogger('cli');

const CONST = require('../resources/const.json');
const Utils = require('./utils');
const hub = require('../event_hub');

module.exports = class Timex extends EventEmitter {

    constructor(cfg) {
        super();

        this.interval = cfg.interval;
        this.tag = 'TIMEX';
        this.watchId = null;
    }

    /**
     * специфическая работа
     * @return {boolean} - запуск без паузы повторной обработки (без использования таймера) 
     */
    async handle() {
        return false;
    }

    async start() {
        const context = this;
        this.watchId = setImmediate(this.execute, context);
    }

    async execute(context) {
        if (context.watchId != null) {
            clearInterval(context.watchId);
            context.watchId = null;
        }

        let do_next = true;
        while (do_next) {
            do_next = await context.handle();
        }

        context.watchId = setInterval(context.execute, context.interval, context);
    }

    async onData(pack){
        hub.sendEvent(CONST.EVENTS.ON_NEW_MESSAGE, this, pack);
    }

    async onError(message){
        hub.sendEvent(CONST.EVENTS.ON_RECEIVE_ERR, this, {id: null, data: message});
    }

    async onIdle(data){
        hub.sendEvent(CONST.EVENTS.ON_RECEIVER_IDLE, this, {id: null, data: null});
    }

}