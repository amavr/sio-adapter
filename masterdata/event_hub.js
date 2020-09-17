'use strict';

const log = require('log4js').getLogger('hub');

class EventHub {

    constructor() {
        log.info('READY');
        this.subscribers = {};
    }

    subscribe(eventName, subscriber) {
        if (this.subscribers[eventName] === undefined) {
            this.subscribers[eventName] = [];
            this.subscribers[eventName].push(subscriber);
        }
        /// TODO: проверка существования подписчика в списке
        else {
            this.subscribers[eventName].push(subscriber);
        }
    }

    /// TODO: реализовать при необходимости
    unsubscribe(eventName, subscriber) {
    }


    async sendEvent(eventName, sender, data){
        /// TODO: подумать над регистрацией пары "событие"-"список подписчиков"
        // this.emit(eventName, sender, data);

        const sub_list = this.subscribers[eventName];
        if(sub_list === undefined) return;

        for(const sub of sub_list){
            await sub.onEvent(eventName, sender, data);
        }
    }

}

module.exports = new EventHub();