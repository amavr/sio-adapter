'use strict';

const path = require('path');
const log = require('log4js').getLogger('handler.msg');

const CONST = require('../resources/const.json');
const DBHelper = require('../helpers/db_helper');
const Utils = require('../helpers/utils');

const SourceDoc = require('../models/mdm_src/source_doc');
const IndicatDoc = require('../models/num/indicat');
const VolumeDoc = require('../models/volumes/vol_doc');

const Consumer = require('../framework/consumer');
const FileHelper = require('../helpers/file_helper');

module.exports = class MessageHandler extends Consumer {

    constructor(cfg) {
        super(cfg);

        this.msg_dir = path.join(cfg.work_dir, cfg.msg_dir);
        this.err_dir = path.join(cfg.work_dir, cfg.err_dir);
        this.log_dir = path.join(cfg.work_dir, cfg.log_dir);
        this.dbg_dir = path.join(cfg.work_dir, cfg.dbg_dir);
        this.save_debug = cfg.save_debug;

        this.db_helper = new DBHelper(cfg.db);
    }

    async init() {
        super.init();
        await this.db_helper.init();
        await this.db_helper.execSql('select 1 from dual');
        log.info('READY')
    }

    async processMsg(msg) {
        try {
            if (msg instanceof SourceDoc) {
                await this.onMsg61(msg);
            }
            else if (msg instanceof IndicatDoc) {
                await this.onMsg131(msg);
            }
            else if (msg instanceof VolumeDoc) {
                await this.onMsg161(msg);
            }
            else {
                // console.log('UNKNOWN');
            }
        }
        catch (ex) {
            log.error(`${msg.id}\t${ex.message}\tMessageHandler.processMsg()`);
            throw ex;
        }
    }

    async onMsg61(doc) {
        const time1 = new Date().getTime();
        /// ЗАГРУЗКА В SIO_MSG6_1 --------------------------------------------
        const rows_data = doc.getColValues(doc.id);
        const columns = SourceDoc.getColNames();
        const sql = `insert into sio_msg6_1(`
            + columns.join(', ')
            + ') values('
            + columns.map((e, i) => `:${i + 1}`).join(', ')
            + ')';

        const res = await this.db_helper.insertMany(sql, rows_data);

        if (res.batchErrors) {
            log.warn(res.batchErrors);
        }

        return;

        const time2 = new Date().getTime();
        /// ПОИСК ПОЛНЫХ ЦЕПОЧЕК --------------------------------------------
        // const ans_chains = await this.db_helper.findChains(doc, doc.id); /// перенесено в БД

        const time3 = new Date().getTime();
        /// ПОИСК ИЛИ СОЗДАНИЕ ШКАЛ --------------------------------------------
        // const ans_registers = await this.db_helper.findRegisters(doc);

        const time4 = new Date().getTime();
        /// ЗАПУСК ОСНОВНОЙ ОБРАБОТКИ  --------------------------------------------
        const ans_handle = await this.db_helper.handleFile(doc, doc.id);

        const end_time = new Date().getTime();
        console.log(`${doc.id.padStart(10)}\ttimes: ${time2 - time1}/${time3 - time2}/${time4 - time3}/${end_time - time4} total:${end_time - time1} msec`);
    }

    async onMsg131(doc) {
        return;

        const answer = await this.db_helper.saveIndicat(doc);
        if (this.save_debug) {
            const codes = '.' + Object.keys(answer).join('.');
            const fpath = path.join(this.dbg_dir, doc.id + codes + '.txt');
            await FileHelper.saveObj(fpath, answer);
        }
    }

    async onMsg161(doc) {
        log.debug('receive 16.1')

        const result = {
            att_points: [],
        }

        /// ЗАГРУЗКА В SIO_MSG16_1 --------------------------------------------
        const rows_data = doc.getColValues(doc.id);
        const columns = VolumeDoc.getColNames();
        const sql = `insert into sio_msg16_1(`
            + columns.join(', ')
            + ') values('
            + columns.map((e, i) => `:${i + 1}`).join(', ')
            + ')';

        const res = await this.db_helper.insertMany(sql, rows_data);

        if (res.batchErrors) {
            log.warn(res.batchErrors);
        }

        return;


        for (const sup_point of doc.nodes) {
            /// отладочная информация
            const sup_info = {
                balance: sup_point.kod_attpoint,
                values: sup_point.values
                // readings: volumes
            };

            /// open for changes
            const ans_open = await this.db_helper.accept_priem(sup_point.kod_attpoint, true);
            sup_info.open = ans_open;

            // ans_open.code = 200;
            // ans_open.ym = 2020.05;

            /// служит для определения куда записывать отладочный файл
            let codes = {};

            if (codes[ans_open.code] === undefined) {
                codes[ans_open.code] = true;
            }

            if (ans_open.code === 200) {

                if (ans_open.ym !== null) {
                    sup_info.values = [];
                    /// update volumes
                    for (const key in sup_point.values) {
                        const val = sup_point.values[key];
                        const ans = await this.db_helper.set_priem_values(
                            val.prev_reg_key,
                            val.last_reg_key,
                            ans_open.ym,
                            val.readings[CONST.LOSSES.KEY_PU],
                            val.readings[CONST.LOSSES.KEY_POTERI],
                            val.readings[CONST.LOSSES.KEY_RAS],
                            val.readings[CONST.LOSSES.KEY_DOP],
                            val.readings[CONST.LOSSES.KEY_OB],
                            val.readings[CONST.LOSSES.KEY_INT]
                        );

                        if (codes[ans.code] === undefined) {
                            codes[ans.code] = true;
                        }
                        ans.key = key;
                        sup_info.values.push(ans)
                    }

                    // , p_pu            in nr_priem.outcounter%type     -- по ПУ = outcounter
                    // , p_poteri        in nr_priem.potcounter%type     -- Потери = potcounter
                    // , p_ras           in nr_priem.outadd%type         -- Расчетный = + outadd
                    // , p_dop           in nr_priem.outadd%type         -- Дополнительный = + outadd
                    // , p_ob            in number                       -- Общий = + outadd
                    // , p_int           in number                       -- Интервальный - никуда ???



                    if (codes[ans_close.code] === undefined) {
                        codes[ans_close.code] = true;
                    }
                    sup_info.close = ans_close;
                }
            }
            /// close for changes
            const ans_close = await this.db_helper.accept_priem(sup_point.kod_attpoint, false);

            result.att_points.push(sup_info);

            const fpath = path.join(this.dbg_dir, pack.id);
            await FileHelper.saveObj(fpath, result);
        }

    }

    async onMessageErr(pack) {
        if (pack) {
            log.error(pack.error);
            const fname = pack.id ? pack.id : FileHelper.getTimeFilename('.log');
            const fpath = path.join(this.err_dir, fname);
            FileHelper.saveObj(fpath, pack);
        }
    }

}