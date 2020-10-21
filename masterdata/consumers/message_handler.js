'use strict';

const path = require('path');
const oracledb = require('oracledb');

const CONST = require('../resources/const.json');
const DBHelper = require('../helpers/db_helper');
const SqlHolder = require('../helpers/sql_holder');
const OraAdapter = require('../adapters/oracle/ora-adp');

const MdmDoc = require('../models/mdm/mdm_doc');
const IndicatDoc = require('../models/indicates/ind_doc');
const VolumeDoc = require('../models/volumes/vol_doc');
const CfgDoc = require('../models/mdm_cfg/cfg_doc');

const Consumer = require('../framework/consumer');
const FileHelper = require('../helpers/file_helper');
const DBRefs = require('../helpers/db_refs');

module.exports = class MessageHandler extends Consumer {

    constructor(cfg) {
        super(cfg);

        this.msg_dir = path.join(cfg.work_dir, cfg.msg_dir);
        this.err_dir = path.join(cfg.work_dir, cfg.err_dir);
        this.log_dir = path.join(cfg.work_dir, cfg.log_dir);
        this.dbg_dir = path.join(cfg.work_dir, cfg.dbg_dir);

        this.debug_61 = cfg.debug_61
        this.debug_131 = cfg.debug_131
        this.debug_161 = cfg.debug_161
        this.handle_61 = cfg.handle_61
        this.handle_131 = cfg.handle_131
        this.handle_161 = cfg.handle_161

        this.db_helper = new DBHelper(cfg.db);
        this.adapter = new OraAdapter();
        this.db_refs = new DBRefs();
    }

    async init() {
        super.init();
        await this.db_helper.init();
        await this.db_refs.init(this.db_helper);
        // await this.db_helper.execSql('select 1 from dual');
        this.adapter.init(this.db_helper.pool);
        this.log.info('READY')
    }

    async processMsg(msg) {
        try {
            if (msg instanceof MdmDoc) {
                await this.onMsg61(msg);
            }
            else if (msg instanceof IndicatDoc) {
                await this.onMsg131(msg);
            }
            else if (msg instanceof VolumeDoc) {
                await this.onMsg161(msg);
            }
            else if (msg instanceof CfgDoc) {
                if (msg.tag === '5.1') {
                    await this.onMsg51(msg);
                }
                else if (msg.tag === '5.5') {

                }
            }
            else {
                // console.log('UNKNOWN');
            }
        }
        catch (ex) {
            this.log.error(`${msg.id}\t${ex.message}\tMessageHandler.processMsg()`);
            throw ex;
        }
    }

    async onMsg61(doc) {
        const time1 = new Date().getTime();
        /// ЗАГРУЗКА В SIO_MSG6_1 --------------------------------------------
        const rows_data = doc.getColValues(doc.id);
        const columns = MdmDoc.getColNames();
        const sql = `insert into sio_61(`
            + columns.join(', ')
            + ') values('
            + columns.map((e, i) => `:${i + 1}`).join(', ')
            + ')';

        const res = await this.db_helper.insertMany(sql, rows_data);

        if (res.batchErrors) {
            this.log.warn(res.batchErrors);
        }

        if (this.handle_61) {

            await this.adapter.handle61(doc);

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
    }

    async onMsg131(doc) {

        if (this.handle_131) {

            const answer = await this.db_helper.saveIndicat(doc);

            if (this.debug_131) {
                const codes = '.' + Object.keys(answer).join('.');
                const fpath = path.join(this.dbg_dir, doc.id + codes + '.txt');
                await FileHelper.saveObj(fpath, answer);
            }
        }
    }

    async onMsg161(doc) {
        this.log.debug('receive 16.1')

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
            this.log.warn(res.batchErrors);
        }

        if (this.handle_161) {
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

                if (this.debug_161) {
                    const fpath = path.join(this.dbg_dir, pack.id);
                    await FileHelper.saveObj(fpath, result);
                }
            }
        }
    }

    async onMsg51(doc) {

        // const xbinds = {
        //     x: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: 10 },
        //     y: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: 20 },
        //     a: { type: oracledb.STRING, dir: oracledb.BIND_INOUT, val: 'www' },
        //     dt: { type: oracledb.DATE, dir: oracledb.BIND_IN, val: new Date() },
        //     res: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT, val: null },
        // }
        // const xanswer = await this.db_helper.callProc('DBG_TOOLS.test', xbinds);
        // this.debug(xanswer);
        // return;

        const beg_dt = new Date();
        const sql = SqlHolder.get('pu_get_point_info');
        const binds = { PNT_KOD_POINT: doc.pnt_kod_point };
        let answer = await this.db_helper.execSql(sql, binds);
        const end_dt = new Date();
        console.log(`5.1 TIME: ${end_dt - beg_dt} msec`);

        if (answer.data.length === 0) {
            log.error(`Point for 5.1 not found. Message @id = "${doc.id}"`);
            return;
        }
        doc.flow = answer.data[0].FLOW_TYPE;
        doc.point_id = answer.data[0].KOD_POINT;
        doc.numobj_id = answer.data[0].KOD_NUMOBJ;
        doc.pu_id = null;

        for (const pu of doc.nodes) {

            if(pu.registers.length === 0) continue;
            const before_dot = pu.registers[0].ini_razr;
            const after_dot = pu.registers[0].ini_razr2;

            let res_code = 0;
            let res_data = '';

            const binds = {
                kod_point_pu_: { type: oracledb.NUMBER, dir: oracledb.BIND_INOUT, val: doc.pu_id },
                kod_point_: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: doc.point_id },
                kod_numobj_: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: doc.numobj_id },
                num_: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: pu.num },
                // type_: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: 16583 },
                // model_: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: null },
                god_vip_: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: pu.issue_year },
                dat_pp_: { type: oracledb.DATE, dir: oracledb.BIND_IN, val: pu.dt_check },
                mpi_: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: pu.mpi },
                ini_razr_: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: before_dot },
                ini_razr2_: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: after_dot },
                dat_s_: { type: oracledb.DATE, dir: oracledb.BIND_IN, val: pu.dt_install },
                dat_po_: { type: oracledb.DATE, dir: oracledb.BIND_IN, val: null },
                flow_type_: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: doc.flow },
                st_: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT, val: res_code },
                errM_: { type: oracledb.STRING, dir: oracledb.BIND_OUT, val: res_data }
            }

            // const binds = {
            //     kod_point_pu_: doc.pu_id,
            //     kod_point_: doc.point_id,
            //     kod_numobj_: doc.numobj_id,
            //     num_: pu.num,
            //     type_: 16583,
            //     model_: null,
            //     god_vip_: pu.issue_year,
            //     dat_pp_: pu.dt_check,
            //     mpi_: pu.mpi,
            //     ini_razr_: before_dot,
            //     ini_razr2_: after_dot,
            //     dat_s_: pu.dt_install,
            //     dat_po_: null,
            //     flow_type_: doc.flow,
            //     st_: res_code,
            //     errM_: res_data
            // };

            answer = await this.db_helper.callProc('IEG_ISE_POINT.insOrUpd_Pu_', binds);
            this.debug(answer);
        }
    }

    async onMessageErr(pack) {
        if (pack && this.save_debug) {
            this.log.error(pack.error);
            const fname = pack.id ? pack.id : FileHelper.getTimeFilename('.log');
            const fpath = path.join(this.err_dir, fname);
            FileHelper.saveObj(fpath, pack);
        }
    }

}