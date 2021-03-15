'use strict';

const path = require('path');
const oracledb = require('oracledb');

const CONST = require('../resources/const.json');
const SqlHolder = require('../helpers/sql_holder');

const FileHelper = require('../helpers/file_helper');
const db_helper = require('../helpers/db_helper');
const db_refs = require('../helpers/db_refs');

const Consumer = require('../framework/consumer');
const BaseMsg = require('../framework/base_msg');

const Statistics = require('./statistics');

const MdmDoc = require('../models/mdm/mdm_doc');
const IndicatDoc = require('../models/indicates/ind_doc');
const VolumeDoc = require('../models/volumes/vol_doc');
const CfgDoc = require('../models/mdm_cfg/cfg_doc');

module.exports = class MessageHandler extends Consumer {

    constructor(cfg, statObj) {
        super(cfg);

        this.msg_dir = path.join(cfg.work_dir, cfg.msg_dir);
        this.dbg_dir = path.join(cfg.work_dir, cfg.dbg_dir);
        FileHelper.checkDir(this.msg_dir);
        FileHelper.checkDir(this.dbg_dir);

        this.carantine_61 = cfg.carantine_61;
        this.carantine_131 = cfg.carantine_131;
        this.carantine_161 = cfg.carantine_161;
        this.job_enabled = cfg.job_enabled;

        this.debug_61 = cfg.debug_61
        this.debug_131 = cfg.debug_131
        this.debug_161 = cfg.debug_161
        this.handle_61 = cfg.handle_61
        this.handle_131 = cfg.handle_131
        this.handle_161 = cfg.handle_161

        this.idle_seconds = cfg.idle_seconds;
        this.last_msg_time = new Date(2000, 0, 1);
        /** требуется запуск джоба на IDLE или он уже срабатывал? */
        this.need_check_db = true;

        this.db_helper = db_helper;
        this.db_refs = db_refs;

        this.stat = statObj;
    }

    async init() {
        super.init();
        // await this.db_helper.execSql('select 1 from dual');
        await this.stat.init(this.db_helper);
        BaseMsg.log = this.log;
    }

    buildMessage(pack) {
        let msg = null;
        if (pack === null) {
            return msg;
        }
        /// IDLE
        else if (pack.id === null && pack.data === null) {
            return msg;
        }
        else {
            if (pack.code === 200) {
                try {
                    const jobj = JSON.parse(pack.data);
                    jobj.id = pack.id;
                    const ies_type = jobj['@type'] ? jobj['@type'] : CONST.MSG_TYPES.NO_TYPE;

                    if (ies_type === CONST.MSG_TYPES.TYPE_MDM) {
                        msg = new MdmDoc(jobj);
                    } else if (ies_type === CONST.MSG_TYPES.TYPE_IND) {
                        msg = new IndicatDoc(jobj);
                    } else if (ies_type === CONST.MSG_TYPES.TYPE_VOL) {
                        msg = new VolumeDoc(jobj);
                    } else if (ies_type === CONST.MSG_TYPES.TYPE_CFG) {
                        msg = new CfgDoc(jobj);
                    } else {
                        msg = MessageHandler.makeErrorMsg(pack, ies_type ? 'UNKNOWN-TYPE: ' + ies_type : 'MSG TYPE NOT DEFINED');
                        FileHelper.save(path.join(this.msg_dir, pack.id), pack.data);
                        this.warn(`${msg.id}\t${msg.error}`);
                    }
                    this.info(`${pack.id} [${msg.tag}]`);
                    this.stat.processMsg(msg);
                    return msg;

                }
                catch (ex) {
                    this.error(`${pack.id}\t${ex.message}\tin buildMessage()`);
                    if (pack.id) {
                        FileHelper.save(path.join(this.msg_dir, pack.id), pack.data);
                    }
                    msg = MessageHandler.makeErrorMsg(pack, ex.message);
                    this.error(`${msg.id ? msg.id : ''}\t${msg.code}\t${msg.error}`);
                }
            }
            else {
                if (pack.id) {
                    FileHelper.save(path.join(this.msg_dir, pack.id), pack.data);
                }
                msg = MessageHandler.makeErrorMsg(pack, pack.data ? pack.data : 'UNCORRECTED');
                this.error(`${msg.id ? msg.id : ''}\t${msg.code}\t${msg.error}`);
            }
            return msg;
        }
    }

    static makeErrorMsg(pack, error) {
        const msg = new BaseMsg(pack);
        msg.id = pack.id;
        msg.tag = 'ERROR';
        msg.raw = pack.data;
        msg.error = error;
        msg.code = pack.code;
        return msg;
    }

    async processMsg(pack) {
        let msg = null;
        try {
            msg = this.buildMessage(pack);
        }
        catch (ex) {
            this.error(ex.message);
            return msg;
        }

        try {
            // IDLE
            if (msg == null) {
                if (this.need_check_db && this.idle_seconds <= (new Date().getTime() - this.last_msg_time.getTime()) / 1000) {
                    await this.onIndle();
                }
            }
            else {
                this.last_msg_time = new Date();

                let need_to_save = false;

                if (msg instanceof MdmDoc) {
                    this.need_check_db = true;
                    need_to_save = await this.onMsg61(msg);
                }
                else if (msg instanceof IndicatDoc) {
                    need_to_save = await this.onMsg131(msg);
                }
                else if (msg instanceof VolumeDoc) {
                    this.need_check_db = true;
                    need_to_save = await this.onMsg161(msg);
                }
                else if (msg instanceof CfgDoc) {
                    if (msg.tag === '5.1') {
                        need_to_save = await this.onMsg51(msg);
                    }
                    else if (msg.tag === '5.5') {
                        need_to_save = await this.onMsg55(msg);
                    }
                }
                else {
                    need_to_save = true;
                    // console.log('UNKNOWN');
                }

                if (need_to_save && pack.id !== null) {
                    FileHelper.save(path.join(this.msg_dir, pack.id), pack.data);
                }
            }
        }
        catch (ex) {
            FileHelper.save(path.join(this.msg_dir, pack.id), pack.data);
            this.error(`${msg.id}\t${ex.message}\tMessageHandler.processMsg()`);
            throw ex;
        }
    }

    /// запуск пакетной обработки
    async onIndle(doc) {
        if (this.need_check_db && this.job_enabled) {
            await this.db_helper.startHandle();
        }
        this.need_check_db = false;
    }

    async onMsg61(doc) {
        let need_to_save = false;

        const time1 = new Date().getTime();
        /// ЗАГРУЗКА В SIO_MSG6_1 --------------------------------------------
        const columns = MdmDoc.getColNames();
        const rows_data = doc.getColValues(doc.id);

        if (doc.errors.length > 0) {
            await this.db_helper.saveErrors('PARSE-6.1', doc.id, doc.errors);
            return true;
        }

        const sql = `insert into ${this.carantine_61}(`
            + columns.join(', ')
            + ') values('
            + columns.map((e, i) => `:${i + 1}`).join(', ')
            + ')';

        // rows_data[1][6] = null;
        // for(const row of rows_data){
        //     row[6] = null;
        // }

        let res = await this.db_helper.insertMany(sql, rows_data);

        if (!res.success) {
            this.error(`${doc.id}\t${res.error}`);
            return true; // исходный файл сохранить
        }

        if (res.batchErrors) {
            this.warn(res.batchErrors);
            need_to_save = true;
        }

        /// ЗАГРУЗКА ТРАНЗИТНЫХ ТОЧЕК В SIO_POINT_CHAINS
        const trans_rows = doc.transit.map(r => [r.parent_id, r.child_id, r.dir, r.type, r.calc_method]);
        const sql_trans = 'INSERT INTO SIO_TRANSIT(MASTER_IES, DETAIL_IES, KOD_DIRECTEN, DETAIL_TYPE, CALC_METHOD) VALUES(:1, :2, :3, :4, :5)';
        res = await this.db_helper.insertMany(sql_trans, trans_rows);

        /// ЗАГРУЗКА СВЯЗЕЙ ТОЧЕК БАЛАНСА (ТУТБ) И ТОЧЕК УЧЕТА (ТУ) ИЗ РАСЧЕТНОЙ СХЕМЫ
        if (doc.balance_points && doc.balance_points.length > 0) {
            const sql_scheme = 'INSERT INTO SIO_SCHEME_POINTS(BALANCE_POINT, PNT_KOD_POINT) VALUES(:1, :2)';
            await this.db_helper.insertMany(sql_scheme, doc.balance_points);
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

        return need_to_save;
    }

    async onMsg131(doc) {
        let need_to_save = false;

        /// ЗАГРУЗКА В SIO_MSG13_1 --------------------------------------------
        const rows_data = doc.getColValues(doc.id);
        const columns = IndicatDoc.getColNames();
        const col_names = columns.join(', ');
        const col_nums = columns.map((e, i) => `:${i + 1}`).join(', ');
        const sql_base_tab = `insert into ${this.carantine_131}(${col_names}) values(${col_nums})`;
        let res = await this.db_helper.insertMany(sql_base_tab, rows_data);


        if (this.handle_131) {

            const answer = await this.db_helper.saveIndicat(doc);
            need_to_save = !answer.success;

            if (this.debug_131) {
                const codes = '.' + Object.keys(answer).join('.');
                const fpath = path.join(this.dbg_dir, doc.id + codes + '.txt');
                await FileHelper.saveObj(fpath, answer);
            }
        }
        return need_to_save;
    }

    async onMsg161(doc) {
        let need_to_save = false;

        const result = {
            att_points: [],
        }

        /// ЗАГРУЗКА В SIO_MSG16_1 --------------------------------------------
        const rows_data = doc.getColValues(doc.id);
        const columns = VolumeDoc.getColNames();
        const col_names = columns.join(', ');
        const col_nums = columns.map((e, i) => `:${i + 1}`).join(', ');
        const sql_base_tab = `insert into ${this.carantine_161}(${col_names}) values(${col_nums})`;
        let res = await this.db_helper.insertMany(sql_base_tab, rows_data);

        if (res.batchErrors) {
            this.warn(res.batchErrors);
            need_to_save = true;
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

                if (!ans_open.success) {
                    need_to_save = true;
                }

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
                else {
                    need_to_save = true;
                }
                /// close for changes
                const ans_close = await this.db_helper.accept_priem(sup_point.kod_attpoint, false);
                if (!ans_open.success) {
                    need_to_save = true;
                }

                result.att_points.push(sup_info);

                if (this.debug_161) {
                    const fpath = path.join(this.dbg_dir, pack.id);
                    await FileHelper.saveObj(fpath, result);
                }
            }
        }

        return need_to_save;
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

        const not_exec = true;
        if (not_exec) {
            console.warn('5.1 DB processing is disabled');
            console.info('For enable you have comment line at 271 line in message_handler');
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

            if (not_exec) continue;

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

        const not_exec = true;
        if (not_exec) {
            console.warn('5.5 DB processing is disabled');
            console.info('For enable you have comment line at 427 line in message_handler');
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

                if (not_exec) continue;

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

            if (not_exec) continue;

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
            this.error(pack.error);
            const fname = pack.id ? pack.id : FileHelper.getTimeFilename('.txt');
            const fpath = path.join(this.msg_dir, fname);
            FileHelper.saveObj(fpath, pack);
        }
    }


}