'use strict';

const path = require('path');
const log = require('log4js').getLogger('handler.stat');
const moment = require('moment');

const Consumer = require('../framework/consumer');
const CONST = require('../resources/const.json');
const FileHelper = require('../helpers/file_helper');

module.exports = class Statistics extends Consumer {

    constructor(cfg) {
        super(cfg);
        this.save_interval = cfg.save_interval;
        this.stat_path = path.join(cfg.work_dir, cfg.stat_path);
        this.day_label = '';
        this.counters = null;

        this.data = {
            dates: {},
            starts: []
        };
    }

    async init() {
        super.init();
        if (FileHelper.FileExistsSync(this.stat_path)) {
            this.data = await FileHelper.readAsObject(this.stat_path);
        }
        this.data.starts.push(new Date().toISOString().substr(0, 19));
        this.addToday();
        FileHelper.saveObjSync(this.stat_path, this.data);

        const context = this;
        setInterval(async () => {
            await FileHelper.saveObj(context.stat_path, context.data);
        }, this.save_interval);

        log.info('READY');
    }

    addToday() {
        const day = new Date().toISOString().substr(0, 10);
        if (this.day_label === day) return;
        this.day_label = day;

        if (this.data.dates[day] === undefined) {
            this.data.dates[day] = {};
        }
        this.counters = this.data.dates[day];
    }

    async executeEvent(pack) {
        const type = 
            pack.data === null ? 'idle' 
            : pack.id === null ? 'error'
            : 'msg';
        await this.addEvent(type);
    }

    async addEvent(eventType) {
        if (this.counters[eventType] === undefined) {
            this.counters[eventType] = 0;
        }
        this.counters[eventType]++;
    }
}
