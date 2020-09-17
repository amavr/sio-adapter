'use strict';

const path = require('path');
const log = require('log4js').getLogger('handler.stat');
const moment = require('moment');
const CONST = require('./resources/const.json');
const cfg = require('../config');
const FileHelper = require('./helpers/file_helper');
const { initOracleClient } = require('oracledb');

class Statistics {

    constructor() {
        this.interval = cfg.options.stat.interval;
        this.stat_path = cfg.options.stat.stat_path;
        this.day_label = '';
        this.counters = null;

        this.data = {
            dates: {},
            times: []
        };

        this.init();
    }

    check_date() {
        const day = new Date().toISOString().substr(0, 10);
        if (this.day_label === day) return;
        this.day_label = day;

        if (this.data.dates[day] === undefined) {
            this.data.dates[day] = {};
        }
        this.counters = this.data.dates[day];
    }

    init() {
        if (FileHelper.FileExists(this.stat_path)) {
            this.data = FileHelper.readAsObjectSync(this.stat_path);
        }
        this.data.starts.push(new Date().toISOString().substr(0, 19));
        this.check_date();
        FileHelper.saveObjSync(this.stat_path, this.data);

        const context = this;
        setInterval(async () => {
            await FileHelper.saveObjAsync(context.stat_path, context.data);
        }, this.interval);

        log.info('READY');
    }

    async onEvent(eventName, sender, data) {
        // console.log(`event: ${eventName}, ${sender.constructor.name}`);
        await this.addEvent(eventName, sender);
    }

    async addEvent(eventName, sender) {
        if (this.counters[eventName] === undefined) {
            this.counters[eventName] = 0;
        }
        this.counters[eventName]++;
    }
}

module.exports = new Statistics();