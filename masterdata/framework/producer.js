'use strict';

const EventEmitter = require('events');
const Utils = require('../helpers/utils');
const log = require('log4js').getLogger('producer');

const CONST = require('../resources/const.json');
const hub = require('./event_hub');
const msgFactory = require('./msg_factory');

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
            this.timer = setTimeout(this.execute, this.interval, this)
            hub.registerSender(this);
        }
    }

    async execute(context) {
        /// сброс таймера если есть
        if (context.timer != null) {
            clearTimeout(context.timer);
            context.timer = null;
        }

        /// работает только когда hub готов принимать
        while (true) {
            /// ожидание когда все потребители освободятся
            while (!hub.allConsumersReady) {
                await Utils.sleep(100);
            }

            const pack = await context.handle();
            if (pack === null) {
                log.debug('IDLE');
                await context.onIdle();
                break;
            }
            try {

                /// 
                const msg = msgFactory.createMsg(pack);

                /// последовательное выполнение
                // await context.onData(msg);

                /// параллельное выполнение
                context.onData(msg);

                // log.debug('send data');
            }
            catch (ex) {
                log.error(ex.message);
                pack.code = 400;
                pack.data = ex.message;
                await context.onError(pack);
                break;
            }
        }

        context.timer = setTimeout(context.execute, context.interval, context);
    }

    async onData(msg) {
        await hub.sendEvent(msg);
    }

    async onError(pack) {
        await hub.sendEvent(pack);
    }

    async onIdle() {
        await hub.sendEvent({ id: null, code: 204, data: null });
    }

}