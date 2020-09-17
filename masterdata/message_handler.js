'use strict';

const path = require('path');
const log = require('log4js').getLogger('handler.msg');

const CONST = require('./resources/const.json');
const db_helper = require('./helpers/db_helper');
const Utils = require('./helpers/utils');

const SourceDoc = require('./models/mdm_src/source_doc');
const IndicatDoc = require('./models/num/indicat');
const VolumeDoc = require('./models/volumes/vol_doc');

const cfg = require('../config');
const FileHelper = require('./helpers/file_helper');

module.exports = class MessageHandler {

    constructor(cfg) {
        this.cfg = cfg;
    }

    async init(){
        await db_helper.init();
        await db_helper.execSql('select 1 from dual');
        log.info('READY')
    }

    async onEvent(eventName, sender, data){
        console.log(`HANDLER\t${eventName}`);
        await this.executeEvent(data);
    }

    async executeEvent(pack) {
        // log.debug('MessageHandler.onMessage');
        try {
            if (pack.code === 200) {
                pack.data = JSON.parse(pack.data);
                const ies_type = pack.data['@type'];

                if (ies_type === CONST.MSG_TYPES.TYPE_MDM) {
                    log.info(' 6.1 arrived');
                    await this.onMessage61(pack);
                } else if (ies_type === CONST.MSG_TYPES.TYPE_IND) {
                    log.info('13.1 arrived');
                    await this.onMessage131(pack);
                } else if (ies_type === CONST.MSG_TYPES.TYPE_VOL) {
                    log.info('16.1 arrived');
                    await this.onMessage161(pack);
                } else {
                    pack.error = 'UNKNOWN-TYPE arrived';
                    log.error(pack.error);
                    await this.onMessageErr(pack);
                }
            }
            else {
                pack.error = 'NON-JSON arrived';
                await this.onMessageErr(pack);
            }
        }
        catch (ex) {
            pack.error = ex.message;
            await this.onMessageErr(pack);
        }
    }

    async onMessage61(pack) {
        const msg = pack.data;
        const fname = pack.id ? pack.id : 'NONAME.' + Utils.getTimeLabel() + '.txt';
        try {
            const doc = new SourceDoc(msg);

            const time1 = new Date().getTime();
            /// ЗАГРУЗКА В SIO_MSG6_1 --------------------------------------------
            const rows_data = doc.getColValues(fname);
            const columns = SourceDoc.getColNames();
            const sql = `insert into sio_msg6_1(`
                + columns.join(', ')
                + ') values('
                + columns.map((e, i) => `:${i + 1}`).join(', ')
                + ')';

            const res = await db_helper.insertMany(sql, rows_data);

            if (res.batchErrors) {
                log.warn(res.batchErrors);
            }

            const time2 = new Date().getTime();
            /// ПОИСК ПОЛНЫХ ЦЕПОЧЕК --------------------------------------------
            const ans_chains = await db_helper.findChains(doc, fname);

            return;


            const time3 = new Date().getTime();
            /// ПОИСК ИЛИ СОЗДАНИЕ ШКАЛ --------------------------------------------
            const ans_registers = await db_helper.findRegisters(doc);

            const time4 = new Date().getTime();
            /// ЗАПУСК ОСНОВНОЙ ОБРАБОТКИ  --------------------------------------------
            const ans_handle = await db_helper.handleFile(doc, fname);

            const end_time = new Date().getTime();
            console.log(`${file.padStart(10)}\ttimes: ${time2 - time1}/${time3 - time2}/${time4 - time3}/${end_time - time4} total:${end_time - time1} msec`);
        }
        catch (ex) {
            log.error(ex);
            const fpath = path.join(this.cfg.err_dir, fname + '.txt');
            FileHelper.saveObjAsync(fpath, msg);
        }
    }

    async onMessage131(pack) {
        log.debug('receive 13.1');
        const msg = pack.data;
        const doc = IndicatDoc.parse(msg);
        const answer = await db_helper.saveIndicat(doc);
        if (cfg.options.saveResults) {
            const codes = '.' + Object.keys(answer).join('.');
            const fpath = path.join(cfg.dbg_dir, pack.id + codes + '.txt');
            await FileHelper.saveObjAsync(fpath, answer);
        }
    }

    async onMessage161(pack) {
        log.debug('receive 16.1')
        const msg = pack.data;

        const result = {
            att_points: [],
        }

        const doc = new VolumeDoc(msg);
        for (const sup_point of doc.sup_points) {
            /// отладочная информация
            const sup_info = {
                balance: sup_point.kod_attpoint,
                values: sup_point.values
                // readings: volumes
            };

            /// open for changes
            const ans_open = await db_helper.accept_priem(sup_point.kod_attpoint, true);
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
                        const ans = await db_helper.set_priem_values(
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
            const ans_close = await db_helper.accept_priem(sup_point.kod_attpoint, false);

            result.att_points.push(sup_info);

            const fpath = path.join(cfg.dbg_dir, pack.id);
            await FileHelper.saveObjAsync(fpath, result);
        }

    }

    async onMessageErr(pack) {
        if (pack) {
            log.error(pack.error);
            const fname = pack.id ? pack.id : FileHelper.getTimeFilename('.log');
            const fpath = path.join(this.cfg.err_dir, fname);
            FileHelper.saveObjAsync(fpath, pack);
        }
    }

}