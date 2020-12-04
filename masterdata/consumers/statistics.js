'use strict';

const path = require('path');
const moment = require('moment');

const BaseMsg = require('../framework/base_msg');

module.exports = class Statistics {

    constructor(cfg, dbHelper) {
        this.save_at_second = cfg.save_at_second;
        this.counters = {};

        this.db_helper = dbHelper;
    }

    async saveStartEvent() {
        const msg = new BaseMsg({ id: 'START' });
        const counters = msg.getCountersData();
        counters.start = 1;
        this.addCounters(msg);
        await this.saveCounters(this);
    }

    async processMsg(msg) {
        if (msg === null) return;
        if (msg.id === null) return;

        this.addCounters(msg);
    }

    addCounters(msg) {
        const msg_counters = msg.getCountersData();

        /// нет счетчиков для метки сообщения
        if (this.counters[msg.tag] === undefined) {
            this.counters[msg.tag] = msg_counters;
        }
        else {
            Object.keys(msg_counters).forEach((key) => {
                if (this.counters[msg.tag][key] === undefined) {
                    this.counters[msg.tag][key] = msg_counters[key];
                }
                else {
                    this.counters[msg.tag][key] += msg_counters[key];
                }
            });
        }
    }

    calcTimeout(context) {
        const seconds = new Date().getSeconds();
        let x = seconds > context.save_at_second 
            ? 60 - (seconds - context.save_at_second)
            : context.save_at_second - seconds;

        if(x < 3) {
            x = 60 + x;
        }

        // console.log(x);
        return x * 1000;
    }

    async saveAt(context, msec) {
        setTimeout(async () => {
            await context.saveCounters(context);
        }, msec);
    }

    async saveCounters(context) {
        try {
            const res = await context.db_helper.saveCounters(context.counters);
            if (res.batchErrors) {
                context.log.warn(res.batchErrors);
            }
            /// сброс счетчиков
            context.counters = {};
        }
        catch (ex) {
            context.log.error(ex.message);
        }
        context.saveAt(context, context.calcTimeout(context));
    }
}
