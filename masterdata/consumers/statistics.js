'use strict';

const path = require('path');
const log = require('log4js').getLogger('handler.stat');
const moment = require('moment');

const Consumer = require('../framework/consumer');
const BaseMsg = require('../framework/base_msg');

const DBHelper = require('../helpers/db_helper');
const FileHelper = require('../helpers/file_helper');
const Utils = require('../helpers/utils');

module.exports = class Statistics extends Consumer {

    constructor(cfg) {
        super(cfg);
        this.save_at_second = cfg.save_at_second;
        this.counters = {};

        this.db_helper = new DBHelper(cfg.db);
    }

    async init() {
        super.init();

        // initFiles();

        await this.db_helper.init();
        await this.db_helper.execSql('select 1 from dual');

        await this.saveStartEvent();

        // this.saveAt(this, this.calcTimeout(this));

        log.info('READY');
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
                log.warn(res.batchErrors);
            }
            /// сброс счетчиков
            context.counters = {};
        }
        catch (ex) {
            log.error(ex.message);
        }
        context.saveAt(context, context.calcTimeout(context));
    }
}
