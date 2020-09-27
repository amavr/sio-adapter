'use strict';

const EventEmitter = require('events');
const Utils = require('../helpers/utils');
const log = require('log4js').getLogger('producer');

const CONST = require('../resources/const.json');
const hub = require('./event_hub');

module.exports = class Producer extends EventEmitter {

    constructor(cfg) {
        super();

        this.interval = cfg.interval;
        this.tag = cfg.tag;
        this.enabled = cfg.enabled;

        this.timer = null;
    }

    /**
     * специфическая работа
     * @return {boolean} - запуск без паузы повторной обработки (без использования таймера) 
     */
    async handle() {
        return false;
    }

    async start() {
        if (this.enabled) {
            const context = this;
            this.timer =  setTimeout(this.execute, this.interval, this)
            hub.registerSender(this);
        }
    }

    // async onReady(context) {
    //     await this.execute(context);
    // }

    async execute(context) {
        /// сброс таймера если есть
        if (context.timer != null) {
            clearTimeout(context.timer);
            context.timer = null;
        }

        /// работает только когда hub готов принимать
        while (true) {
            /// ожидание когда все потребители освободятся
            while(!hub.allConsumersReady){
                await Utils.sleep(100);
            }

            try {
                const pack = await context.handle();
                if (pack === null) {
                    await context.onIdle();
                    break;
                }
                /// последовательное выполнение
                // await context.onData(pack);

                /// параллельное выполнение
                context.onData(pack);

                // log.debug('send data');
            }
            catch (ex) {
                log.error(ex.message);
                break;
            }
        }

        context.timer = setTimeout(context.execute, context.interval, context);
    }

    async onData(pack) {
        await hub.sendEvent(pack);
    }

    async onError(pack) {
        await hub.sendEvent(pack);
    }

    async onIdle() {
        // await hub.sendEvent({ id: null, data: null });
    }

}