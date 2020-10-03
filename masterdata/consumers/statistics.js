'use strict';

const path = require('path');
const log = require('log4js').getLogger('handler.stat');
const moment = require('moment');

const Consumer = require('../framework/consumer');
const BaseMsg = require('./framework/base_msg');

const DBHelper = require('../helpers/db_helper');
const FileHelper = require('../helpers/file_helper');
const Utils = require('../helpers/utils');

module.exports = class Statistics extends Consumer {

    constructor(cfg) {
        super(cfg);
        this.save_interval = cfg.save_interval;
        this.stat_path = path.join(cfg.work_dir, cfg.stat_path);
        this.day_label = '';
        this.counters = {};

        this.db_helper = new DBHelper(cfg.db);
    }

    async init() {
        super.init();

        // initFiles();

        await this.db_helper.init();
        await this.db_helper.execSql('select 1 from dual');

        const context = this;
        setInterval(async () => {
            await this.saveCounters(context);
        }, this.save_interval);

        log.info('READY');
    }

    emptyData() {
        return {
            dates: {},
            starts: []
        }
    }

    async initFile() {
        this.data = this.emptyData();
        if (FileHelper.FileExistsSync(this.stat_path)) {
            try {
                this.data = await FileHelper.readAsObject(this.stat_path);
            }
            catch (ex) {
                log.error(`read stat file error: "${ex.message}" at ${this.stat_path}`);
                FileHelper.moveFileSync(this.stat_path, this.stat_path + '.' + Utils.getTimeLabel() + '.broken');
                this.data = this.emptyData();
            }
        }
        this.data.starts.push(new Date().toISOString().substr(0, 19));
        this.addToday();
        FileHelper.saveObjSync(this.stat_path, this.data);

        const context = this;
        setInterval(async () => {
            await FileHelper.saveObj(context.stat_path, context.data);
        }, this.save_interval);
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
        // if (type !== 'idle') {
        //     await this.addEvent(type);
        // }
    }

    async addEvent(eventType) {
        if (this.counters[eventType] === undefined) {
            this.counters[eventType] = 0;
        }
        this.counters[eventType]++;
    }

    async saveCounters(context) {
        const res = context.db_helper.saveCounters('node', context.counters);
        if(res.batchErrors){
            log.error(res.batchErrors);
        }

        context.counters = {};
    }
}
