'use strict';

const log = require('log4js').getLogger('hub');

class EventHub {

    constructor() {
        log.info('READY');
        this.senders = [];
        this.subs = [];
        this.allConsumersReady = true;
    }

    subscribe(subscriber) {
        if (this.subs.indexOf(subscriber) < 0) {
            this.subs.push(subscriber);
            if (!subscriber.isReady()) {
                this.allConsumersReady = false;
            }
        }
    }

    registerSender(sender) {
        if (this.senders.indexOf(sender) < 0) {
            this.senders.push(sender);
        }
    }

    setBusy() {
        /// сразу установка в неготовность
        this.allConsumersReady = false;
    }

    setReady() {
        /// проверка всех подписчиков на готовность
        this.allConsumersReady = this.subs.every(sub => sub.isReady());
    }

    async sendEvent(msg) {
        /// подписчики на событие
        for (const sub of this.subs) {
            await sub.onData(msg);
        }
    }

}

module.exports = new EventHub();