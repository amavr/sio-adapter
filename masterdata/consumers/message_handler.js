'use strict';

const path = require('path');
const oracledb = require('oracledb');

const CONST = require('../resources/const.json');
const DBHelper = require('../helpers/db_helper');
const SqlHolder = require('../helpers/sql_holder');
const OraAdapter = require('../adapters/oracle/ora-adp');

const FileHelper = require('../helpers/file_helper');
const DBRefs = require('../helpers/db_refs');

const Consumer = require('../framework/consumer');
const BaseMsg = require('../framework/base_msg');

const MdmDoc = require('../models/mdm/mdm_doc');
const IndicatDoc = require('../models/indicates/ind_doc');
const VolumeDoc = require('../models/volumes/vol_doc');
const CfgDoc = require('../models/mdm_cfg/cfg_doc');

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

        this.idle_seconds = cfg.idle_seconds;
        this.last_msg_time = new Date(2000, 0, 1);
        this.need_check_db = true; // требуется срабатывание триггера на IDLE или он уже срабатывал?

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
        BaseMsg.log = this.log;
        this.log.info('READY');
    }

    async processMsg(msg) {
        try {
            // IDLE
            if (msg == null) {
                if (this.need_check_db && this.idle_seconds <= (new Date().getTime() - this.last_msg_time.getTime()) / 1000) {
                    await this.onIndle();
                }
            }
            else {
                this.last_msg_time = new Date();

                if (msg instanceof MdmDoc) {
                    await this.onMsg61(msg);
                    this.need_check_db = true;
                }
                else if (msg instanceof IndicatDoc) {
                    await this.onMsg131(msg);
                }
                else if (msg instanceof VolumeDoc) {
                    await this.onMsg161(msg);
                    this.need_check_db = true;
                }
                else if (msg instanceof CfgDoc) {
                    if (msg.tag === '5.1') {
                        await this.onMsg51(msg);
                    }
                    else if (msg.tag === '5.5') {
                        await this.onMsg55(msg);
                    }
                }
                else {
                    // console.log('UNKNOWN');
                }
            }
        }
        catch (ex) {
            this.log.error(`${msg.id}\t${ex.message}\tMessageHandler.processMsg()`);
            throw ex;
        }
    }

    /// запуск пакетной обработки
    async onIndle(doc) {
        if (this.need_check_db) {
            await this.db_helper.startHandle();
        }
        this.need_check_db = false;
    }

    async onMsg61(doc) {
        const time1 = new Date().getTime();
        /// ЗАГРУЗКА В SIO_MSG6_1 --------------------------------------------
        const rows_data = doc.getColValues(doc.id);
        const columns = MdmDoc.getColNames();
        const sql = `insert into sio_msg6_1(`
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
        const result = {
            errors: [],
            proc_calls: 0
        }


        const beg_dt = new Date();

        if (await this.setSysKeysMsg5(doc) === false) {
            return;
        }

        let res_code = 0;
        let res_data = '';
        let answer = null;

        for (const pu of doc.nodes) {

            const pu_ids = await this.db_helper.existsExtId(pu.id, doc.flow);
            pu.sys_id = pu_ids.length > 0 ? pu_ids[0].ID : null;

            if (pu.registers.length === 0) continue;
            const before_dot = pu.registers[0].ini_razr;
            const after_dot = pu.registers[0].ini_razr2;

            const pu_binds = {
                kod_point_pu_: { type: oracledb.NUMBER, dir: oracledb.BIND_INOUT, val: pu.sys_id },
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

            if (pu.sys_id === null) {
                answer = await this.db_helper.callProc('IEG_ISE_POINT.insOrUpd_Pu_', pu_binds);
                pu.sys_id = answer.data.outBinds.kod_point_pu_;
                await this.db_helper.addPair(pu.sys_id, pu.id, 9, doc.flow, '5.1');
                result.proc_calls++;

                if (answer.error) {
                    this.error(answer.error);
                    result.errors.push({ level: 'PU', type: 'ORACLE', msg: answer.error });
                    continue;
                }
                if (answer.data.outBinds.st_ < 0) {
                    this.error(answer.data.outBinds.errM_);
                    result.errors.push({ level: 'PU', type: 'HANDLE', msg: answer.data.outBinds.errM_ });
                    continue;
                }
            }


            for (const reg of pu.registers) {

                const reg_ids = await this.db_helper.existsExtId(reg.ini_kod_point_ini, doc.flow);
                reg.sys_id = reg_ids.length > 0 ? reg_ids[0].ID : null;

                const reg_binds = {
                    kod_point_ini_: { type: oracledb.NUMBER, dir: oracledb.BIND_INOUT, val: reg.sys_id },
                    kod_point_pu_: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: pu.sys_id },
                    kod_numobj_: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: doc.numobj_id },
                    kod_directen_: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: reg.direct_id },
                    energy_: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: reg.energy_id },
                    kodinterval_: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: reg.interv_id },
                    rkoef_: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: reg.ini_rkoef },
                    flow_type_: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: doc.flow },
                    st_: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT, val: res_code },
                    errM_: { type: oracledb.STRING, dir: oracledb.BIND_OUT, val: res_data }
                }

                if (reg.sys_id === null) {
                    answer = await this.db_helper.callProc('IEG_ISE_POINT.insOrUpd_Ini_', reg_binds);
                    reg.sys_id = answer.data.outBinds.kod_point_ini_;
                    await this.db_helper.addPair(reg.sys_id, reg.ini_kod_point_ini, 10, doc.flow, '5.1');
                    result.proc_calls++;

                    if (answer.error) {
                        this.error(answer.error);
                        result.errors.push({ level: 'INI', type: 'ORACLE', msg: answer.error });
                        continue;
                    }
                    if (answer.data.outBinds.st_ < 0) {
                        this.error(answer.data.outBinds.errM_);
                        result.errors.push({ level: 'INI', type: 'HANDLE', msg: answer.data.outBinds.errM_ });
                        continue;
                    }
                }

                if (reg.ind === undefined) {
                    const err = { level: 'IND', type: 'STRUCT', msg: 'REGISTER W/O INDICATES' };
                    this.error(err.msg);
                    result.errors.push(err);
                    continue;
                }

                const ind_ids = await this.db_helper.existsExtId(reg.ind.id, doc.flow);
                reg.ind.sys_id = ind_ids.length > 0 ? ind_ids[0].ID : null;

                const ind_binds = {
                    IND_ID: { type: oracledb.NUMBER, dir: oracledb.BIND_INOUT, val: reg.ind.sys_id },
                    MSG_IES: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: doc.id },
                    FLOW_TYPE: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: doc.flow },
                    IND_IES: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: reg.ind.id },
                    INI_ID: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: reg.sys_id },
                    PU_ID: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: pu.sys_id },
                    DT: { type: oracledb.DATE, dir: oracledb.BIND_IN, val: new Date(Date.parse(reg.ind.dt)) },
                    VAL: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: reg.ind.value },
                    WAY_ID: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: pu.way_id },
                    // IMP_ID: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: 13.1 },
                    RESULT_CODE: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
                    RESULT_DATA: { type: oracledb.STRING, dir: oracledb.BIND_OUT }
                }

                if (reg.ind.sys_id === null) {
                    answer = await this.db_helper.callProc('IEG_CONSUMER_VOLS.COUNTER_SAVE_LAST', ind_binds);
                    reg.ind.sys_id = answer.data.outBinds.IND_ID;
                    await this.db_helper.addPair(reg.ind.sys_id, reg.ind.id, 30, doc.flow, '5.1');
                    result.proc_calls++;

                    if (answer.error) {
                        this.error(answer.error);
                        result.errors.push({ level: 'IND', type: 'ORACLE', msg: answer.error });
                        continue;
                    }
                    if (answer.data.outBinds.RESULT_CODE < 0) {
                        this.error(answer.data.outBinds.RESULT_DATA);
                        result.errors.push({ level: 'IND', type: 'HANDLE', msg: answer.data.outBinds.RESULT_DATA });
                        continue;
                    }
                }
            }
        }
        const end_dt = new Date();
        console.log(`5.1 TIME: ${end_dt - beg_dt} msec for ${result.proc_calls} SP calls with ${result.errors.length} errors`);
    }

    async onMsg55(doc) {
        const result = {
            errors: [],
            proc_calls: 0
        }

        const beg_dt = new Date();

        if (await this.setSysKeysMsg5(doc) === false) {
            return;
        }

        for (const pu of doc.nodes) {

            let res_code = 0;
            let res_data = '';
            let answer = null;

            for (const reg of pu.registers) {

                const ind_binds = {
                    IND_ID: { type: oracledb.NUMBER, dir: oracledb.BIND_INOUT, val: null },
                    MSG_IES: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: doc.id },
                    FLOW_TYPE: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: doc.flow },
                    IND_IES: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: reg.ind.id },
                    INI_IES: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: reg.ini_kod_point_ini },
                    PU_ID: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: pu.sys_id },
                    DT: { type: oracledb.DATE, dir: oracledb.BIND_IN, val: new Date(Date.parse(reg.ind.dt)) },
                    VAL: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: reg.ind.value },
                    WAY_ID: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: pu.way_id },
                    // IMP_ID: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: 13.1 },
                    RESULT_CODE: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
                    RESULT_DATA: { type: oracledb.STRING, dir: oracledb.BIND_OUT }
                }

                answer = await this.db_helper.callProc('IEG_CONSUMER_VOLS.CLOSE_INI', ind_binds);
                result.proc_calls++;
                if (answer.error) {
                    this.error(answer.error);
                    result.errors.push({ level: 'IND', type: 'ORACLE', msg: answer.error });
                    continue;
                }
                if (answer.data.outBinds.RESULT_CODE < 0) {
                    this.error(answer.data.outBinds.RESULT_DATA);
                    result.errors.push({ level: 'IND', type: 'HANDLE', msg: answer.data.outBinds.RESULT_DATA });
                    continue;
                }
            }

            const pu_binds = {
                kod_point_pu_: { type: oracledb.NUMBER, dir: oracledb.BIND_INOUT, val: pu.sys_id },
                kod_numobj_: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: doc.numobj_id },
                dat_po_: { type: oracledb.DATE, dir: oracledb.BIND_IN, val: pu.dt_uninstall },
                flow_type_: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: doc.flow },
                st_: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT, val: res_code },
                errM_: { type: oracledb.STRING, dir: oracledb.BIND_OUT, val: res_data }
            }

            answer = await this.db_helper.callProc('IEG_ISE_POINT.remove_Pu_', pu_binds);
            result.proc_calls++;
            if (answer.error) {
                this.error(answer.error);
                result.errors.push({ level: 'PU', type: 'ORACLE', msg: answer.error });
                continue;
            }
            if (answer.data.outBinds.st_ < 0) {
                this.error(answer.data.outBinds.errM_);
                result.errors.push({ level: 'PU', type: 'HANDLE', msg: answer.data.outBinds.errM_ });
                continue;
            }
        }


        const end_dt = new Date();
        console.log(`5.5 TIME: ${end_dt - beg_dt} msec for ${result.proc_calls} SP calls with ${result.errors.length} errors`);
    }

    async setSysKeysMsg5(doc) {
        let answer = await this.db_helper.execSql(SqlHolder.get('pu_get_point_info'), { PNT_KOD_POINT: doc.pnt_kod_point });

        if (answer.data.length === 0) {
            log.error(`Point for 5.1 not found. Message @id = "${doc.id}"`);
            return false;
        }
        doc.flow = answer.data[0].FLOW_TYPE;
        doc.point_id = answer.data[0].KOD_POINT;
        doc.numobj_id = answer.data[0].KOD_NUMOBJ;

        for (const pu of doc.nodes) {
            pu.way_id = this.db_refs.link_dicts.IndWays[pu.indicates_way];

            if (doc.tag === '5.5') {
                const rows = await this.db_helper.getSysKeysByExtKey(pu.id);
                if (rows.length > 0) {
                    pu.sys_id = rows[0].ID;
                }
            }

            for (const reg of pu.registers) {
                reg.direct_id = this.db_refs.link_dicts.Direction[reg.ini_kod_directen];
                reg.energy_id = this.db_refs.link_dicts.EnergyKind[reg.ini_energy];
                reg.interv_id = this.db_refs.link_dicts.Intervals[reg.ini_kodinterval];
            }
        }

        return true;
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