'use strict';

const oracledb = require('oracledb');
const log = require('log4js').getLogger('DB.HELPER ');
const Utils = require('./utils');
const SqlHolder = require('./sql_holder');

class DBHelper {

    constructor() {
        oracledb.extendedMetaData = true;
        this.pool = null;
    }

    async init(cfg) {
        this.options = cfg;
        this.options.connectString = this.options.cs.join('\n');
        this.pool = await oracledb.createPool(this.options);
        this.dbname = cfg.cs.join().replace(/.*SERVICE_NAME\s*=\s*(\w+)\W*/gi, '$1');
        try {
            // const dbcon = await this.getConnection();
            // const sql = SqlHolder.get('job_check');
            // await dbcon.execute(sql);
            log.info('READY ' + (this.dbname ? this.dbname.toUpperCase() : 'UNKNOWN'));
        }
        catch (ex) {
            log.error(ex.message);
        }
    }

    async getConnection() {
        return await this.pool.getConnection();
    }

    async close(dbcon) {
        if (dbcon) {
            try {
                await dbcon.close();
            }
            catch (ex) {
                return ex.message;
            }
        }
        return null;
    }

    async commit() {
        await this.pool.commit();
    }

    async rollback() {
        await this.pool.rollback();
    }

    async existsExtId(extId, flowType) {
        const sql = 'select id from ier_link_objects where id_ies = :ext_id and flow_type = :flow';
        const binds = {
            ext_id: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: extId },
            flow: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: flowType }
        };
        const answer = await this.execSql(sql, binds);
        return answer.data;
    }

    async execSql(sql, binds, autoCommit) {
        const res = {
            success: false,
            error: null,
            data: null,
            outBinds: null
        };

        const dbcon = await this.getConnection();
        try {
            const do_commit = autoCommit === undefined ? true : autoCommit;
            const result = await dbcon.execute(sql, binds ? binds : [], { autoCommit: do_commit ? true : false, outFormat: oracledb.OBJECT, extendedMetaData: true });
            res.data = result.rows;
            res.success = true;
        } catch (ex) {
            res.error = ex.message;
        } finally {
            await this.close(dbcon);
        }
        return res;
    }

    async select(sql, binds) {
        const dbcon = await this.getConnection();
        const result = await dbcon.execute(sql, binds, {outFormat: oracledb.OBJECT});
        await this.close(dbcon);
        return result.rows;
    }

    async selectSqlName(sqlName, binds) {
        return await this.select(SqlHolder.get(sqlName), binds);
    }

    async saveCounters(counters) {
        const sql = 'insert into sio_counters_log(tag, code, value) values(:1, :2, :3)';
        const rows = [];
        for (const tag of Object.keys(counters)) {
            const tag_counters = counters[tag];
            for (const key of Object.keys(tag_counters)) {
                rows.push([tag, key, tag_counters[key]]);
            }
        }
        if (rows.length > 0) {
            return await this.insertMany(sql, rows);
        }
        else {
            return {};
        }
    }

    /**
     * Сохраняет ошибки в таблицу SIO_MSG_ERRORS
     * @param {string(32)} tag 
     * @param {string(200)} source 
     * @param {Array<string>} errors 
     */
    async saveErrors(tag, source, errors) {
        const rows = [];
        for (const e of errors) {
            rows.push([tag, source, e]);
        }
        return await this.insertMany(SqlHolder.get('save_errors'), rows);
    }

    /**
     * Пакетная запись в БД.
     * !!! Перед вызовом метода нужно подключиться к базе - db_helper.connect(), а после - отключиться !!!
     * @param {string} sql Запрос в виде 'insert into emp(id, name) values(:1, :2)'
     * @param {Array<Array>} rows 
     * 
     */
    async insertMany(sql, rows) {
        var options = {
            autoCommit: true,   // autocommit if there are no batch errors
            batchErrors: false   // true - продолжение выполнения, даже если существуют ошибки в части записей
        };

        const res = {
            execResult: null,
            success: false,
            error: null
        }

        let dbcon = null;
        while (true) {
            try {
                dbcon = await this.getConnection();
                res.execResult = await dbcon.executeMany(sql, rows, options);
                res.success = res.execResult.rowsAffected === rows.length;
                await dbcon.commit();
            } catch (ex) {
                try {
                    await dbcon.ping();
                }
                catch (ex) {
                    // только в это случае нужен повторный цикл и пауза
                    await this.close(dbcon);
                    await Utils.sleep(3000);
                    continue;
                }
                res.error = ex.message;
                log.error(ex.message);
                log.error(ex.stack);
                await dbcon.rollback();
            }
            await this.close(dbcon);
            break;
        }

        return res;
    }

    static getTypeName(typeId) {
        if (typeId === oracledb.STRING) {
            return 'VARCHAR2';
        }
        else if (typeId === oracledb.NUMBER) {
            return 'NUMBER';
        }
        else if (typeId === oracledb.DATE) {
            return 'DATE';
        }
        else {
            return "-";
        }
    }

    async startHandle() {
        const sql = `BEGIN IEG_CONTROLLER.RUN(:code, :msg); END;`;
        const binds = {
            code: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
            msg: { type: oracledb.STRING, dir: oracledb.BIND_OUT }

        }

        const dbcon = await this.getConnection();
        try {
            log.info(`RUN HANDLE`);
            const res = await dbcon.execute(sql, binds);
            await dbcon.commit();
            log.info(`RUN HANDLE result - code: ${res.outBinds.code}  msg: ${res.outBinds.msg}`);
        }
        catch (ex) {
            log.error(ex.message);
            await dbcon.rollback();
        }
        finally {
            this.close(dbcon);
        }
    }

    async addPair(sysId, extId, objTypeId, flowType, tag) {
        const sql = 'BEGIN IEG_MDM.ADD_PAIR(:sys_id, :ext_id, :type_id, :flow, :tag_code); END;';
        const binds = {
            sys_id: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: sysId },
            ext_id: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: extId },
            type_id: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: objTypeId },
            flow: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: flowType },
            tag_code: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: tag }
        }

        const dbcon = await this.getConnection();
        try {
            const res = await dbcon.execute(sql, binds);
            await dbcon.commit();
        }
        catch (ex) {
            log.error(ex.message);
            await dbcon.rollback();
        }
        finally {
            this.close(dbcon);
        }
    }




    async callProc(procName, binds) {
        const answer = {
            // {'<code>': [{id: '', ret_msg: ''}]}
        };

        const params = [];
        for (const key of Object.keys(binds)) {
            params.push(`${key} => :${key}`);
        }
        const sql = `begin ${procName}(${params.join()}); end;`;
        const dbcon = await this.getConnection();
        try {
            const res = await dbcon.execute(sql, binds, { resultSet: false, autoCommit: false });
            await dbcon.commit();
            answer.data = res;
            answer.success = true;
        }
        catch (ex) {
            answer.error = ex.message;
            answer.success = false;
            // console.error(ex);
            await dbcon.rollback();
        }
        finally {
            this.close(dbcon);
        }
        return answer;
    }

    async getLinksByIES(list) {
        const answer = {
            success: false,
            error: null,
            data: {}
        }
        const binds = {
            ies_list: {
                type: "ASUSETYPES.VARCHAR2$TABLE",
                dir: oracledb.BIND_IN,
                val: list
            },
            row_list: {
                type: "ASUSETYPES.INDEXED_STRING$TABLE",
                dir: oracledb.BIND_OUT,
            }
        };
        try {
            const res = await this.callProcWithArrays('IEG_MDM.FIND_LINKS_BY_IES', binds);
            answer.success = res.success;
            answer.error = res.error;
            if (answer.success) {
                for (const row of res.outBinds.row_list) {
                    if (answer.data[row.VAL] === undefined) {
                        answer.data[row.VAL] = [];
                    }
                    answer.data[row.VAL].push({ id: row.IDX, type: row.VAL2 });
                }
            }
        }
        catch (ex) {
            answer.success = false;
            answer.error = ex.message;
        }
        return answer;
    }

    async callProcWithArrays(procName, binds) {
        const answer = {
            outBinds: null,
            success: false,
            error: null
        };

        const params = [];
        for (const key of Object.keys(binds)) {
            params.push(`${key} => :${key}`);
        }
        const sql = `begin ${procName}(${params.join()}); end;`;
        const dbcon = await this.getConnection();
        try {
            const res = await dbcon.execute(sql, binds, { resultSet: false, autoCommit: false });
            await dbcon.commit();
            answer.outBinds = JSON.parse(JSON.stringify(res.outBinds));
            answer.success = true;
        }
        catch (ex) {
            answer.error = ex.message;
            answer.success = false;
            // console.error(ex);
            await dbcon.rollback();
        }
        finally {
            this.close(dbcon);
        }
        return answer;
    }

    async getSysKeysByExtKey(extKey) {
        const sql = SqlHolder.get('extid_to_sysid');
        const answer = await this.execSql(sql, [extKey], true);
        if (answer.success) {
            return answer.data;
        }
        else {
            return [];
        }
    }

    async saveIndicat(doc) {
        const ans = {
            success: true
            // {'<code>': [{id: '', ret_msg: ''}]}
        };

        // let dbcon = await this.pool.getConnection();
        let code;
        for (const node of doc.nodes) {
            const dbcon = await this.getConnection();
            try {
                code = -500;
                // dbcon = await this.pool.getConnection();
                const res = await dbcon.execute(
                    'begin ' +
                    'ieg_consumer_vols.COUNTER_SAVE_LAST(' +
                    ':id_ies_msg_,' +
                    ':id_ies_indicat_,' +
                    ':id_ies_ini_,' +
                    ':readlast_date_,' +
                    ':readlast_val_,' +
                    ':id_ies_poktype_,' +
                    ':result_code, ' +
                    ':result_msg' +
                    ');' +
                    'end;',
                    {
                        id_ies_msg_: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: doc.ind_id },
                        id_ies_indicat_: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: node.id },
                        id_ies_ini_: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: node.register_id },
                        readlast_date_: { type: oracledb.DATE, dir: oracledb.BIND_IN, val: new Date(Date.parse(node.dt)) },
                        readlast_val_: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: node.value },
                        id_ies_poktype_: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: doc.type },
                        result_code: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
                        result_msg: { type: oracledb.STRING, dir: oracledb.BIND_OUT }
                    },
                    {
                        resultSet: false,
                        autoCommit: false
                    });
                // await dbcon.rollback();
                await dbcon.commit();

                code = res.outBinds.result_code;
                if (!ans[code]) ans[code] = [];
                ans[code].push({ register_id: node.id, msg: res.outBinds.result_msg });
                if (res.outBinds.result_code < 0) {
                    ans.success = false;
                }
            }
            catch (ex) {
                if (!ans[code]) ans[code] = [];
                ans[code].push({ register_id: node.id, msg: ex.message });
                log.error(ex.message);
                // console.error(ex);
                await dbcon.rollback();
                ans.success = false;
            }
            finally {
                this.close(dbcon);
            }
        }
        // this.close(dbcon);
        return ans;
    }

    async findChainsX(doc, fname) {

        const ans = {
            file: fname,
            chains: {}
        };

        let dbcon = null;
        try {
            for (const sup_point of doc.nodes) {
                for (const cnt_point of sup_point.nodes) {
                    for (const cnt_device of cnt_point.nodes) {
                        try {
                            dbcon = await this.getConnection();
                            const sql =
                                'begin ' +
                                'ieg_consumer_mdm.find_chains(' +
                                ':ies_inn,' +
                                ':ies_device_num,' +
                                ':id_ies_contract,' +
                                ':id_ies_consumer,' +
                                ':id_ies_object,' +
                                ':id_ies_connect,' +
                                ':id_ies_point,' +
                                ':id_ies_point_pu,' +
                                ':flow_type,' +
                                ':result_code, ' +
                                ':result_data' +
                                ');' +
                                'end;';

                            const binds = {
                                ies_inn: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: doc.abon_inn },
                                ies_device_num: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: cnt_device.pu_num },
                                id_ies_contract: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: doc.dg_kod_dog },
                                id_ies_consumer: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: doc.abon_kodp },
                                id_ies_object: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: doc.nobj_kod_numobj },
                                id_ies_connect: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: sup_point.attp_kod_attpoint },
                                id_ies_point: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: cnt_point.pnt_kod_point },
                                id_ies_point_pu: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: cnt_device.pu_kod_point_pu },
                                flow_type: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: doc.flow_type },
                                result_code: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
                                result_data: { type: oracledb.STRING, dir: oracledb.BIND_OUT }
                            };

                            const opts = {
                                resultSet: false,
                                autoCommit: true
                            };

                            // const script = DBHelper.getProcCall(sql, binds);
                            // console.log(script);

                            const res = await dbcon.execute(sql, binds, opts);
                            const code = res.outBinds.result_code;
                            if (!ans.chains[code]) ans.chains[code] = [];
                            ans.chains[code].push(JSON.parse(res.outBinds.result_data));

                            // await dbcon.commit();
                        }
                        catch (ex) {
                            if (!ans.chains['error']) ans.chains['error'] = [];
                            ans.chains['error'].push({ err: ex.message });
                            log.error(ex.message);
                            dbcon = await this.getConnection();
                        }
                        finally {
                            this.close(dbcon);
                        }
                    }
                }
            }
        }
        catch (ex) {
            console.error(ex);
        }
        return ans;
    }

    async findRegisters(doc) {

        const ans = {
        };

        try {
            for (const sup_point of doc.nodes) {
                for (const cnt_point of sup_point.nodes) {
                    for (const cnt_device of cnt_point.nodes) {
                        let has_fatal = false;
                        // const dbcon = await this.pool.getConnection();
                        const dbcon = await this.getConnection();
                        try {
                            for (const register of cnt_device.nodes) {
                                // promises.push(
                                const res = await dbcon.execute(
                                    'begin ' +
                                    'ieg_consumer_mdm.find_registers(' +
                                    ':ies_register_id,' +
                                    ':ies_point_pu_id,' +
                                    ':ies_dir,' +
                                    ':ies_kind,' +
                                    ':ies_period,' +
                                    ':ies_coef,' +
                                    ':ies_before_dot,' +
                                    ':ies_after_dot,' +
                                    ':flow_type, ' +
                                    ':register_id, ' +
                                    ':result_code, ' +
                                    ':result_data' +
                                    ');' +
                                    'end;',
                                    {
                                        ies_register_id: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: register.ini_kod_point_ini },
                                        ies_point_pu_id: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: cnt_device.pu_kod_point_pu },
                                        ies_dir: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: register.ini_kod_directen },
                                        ies_kind: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: register.ini_energy },
                                        ies_period: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: register.ini_kodinterval },
                                        ies_coef: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: register.ini_rkoef },
                                        ies_before_dot: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: register.ini_razr },
                                        ies_after_dot: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: register.ini_razr2 },
                                        flow_type: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: doc.flow_type },
                                        register_id: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
                                        result_code: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
                                        result_data: { type: oracledb.STRING, dir: oracledb.BIND_OUT }
                                    },
                                    {
                                        resultSet: false,
                                        autoCommit: false
                                    })
                                // );

                                const code = res.outBinds.result_code;
                                if (!ans[code]) ans[code] = [];
                                let result_data = { msg: "JSON problem with result_data parsing" }
                                try {
                                    result_data = JSON.parse(res.outBinds.result_data);
                                }
                                catch (ex) {
                                    console.log(ex);
                                }
                                ans[code].push(
                                    {
                                        reg_id: res.outBinds.register_id, res_data: result_data
                                    });
                            }

                            const res = await dbcon.execute('begin ieg_consumer_mdm.complete_registers(); end;');
                            await dbcon.commit();
                        }
                        catch (ex) {
                            if (!ans['error']) ans['error'] = [];
                            ans['error'].push({ err: ex.message, info: cnt_device });
                            // await dbcon.rollback();
                            await dbcon.commit();
                            log.error(ex.message);
                            has_fatal = true;
                        }
                        finally {
                            this.close(dbcon);
                        }

                        if (has_fatal) {
                            return ans;
                        }
                    }
                }
            }
        }
        catch (ex) {
            log.error(ex.message);
        }
        return ans;
    }

    async handleFile(doc, filename) {
        let res = true;
        const tran_id = Utils.getHash(doc.abon_kodp + new Date().getTime());
        const dbcon = await this.getConnection();
        try {

            await dbcon.execute(
                'BEGIN ' +
                'IEG_CONSUMER_MDM.PROCESS_FILE(:fname,:block_id,:delete_source); ' +
                'END;',
                {
                    fname: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: filename },
                    block_id: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: tran_id },
                    delete_source: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: 'Y' },
                },
                {
                    resultSet: false,
                    autoCommit: true
                });

            // await dbcon.commit();
        }
        catch (ex) {
            log.error(ex.message);
            res = false;
        }
        finally {
            this.close(dbcon);
        }

        return res;
    }

    async accept_priem(key, doAccept) {
        const ans = {
            success: true,
            code: 200,
            msg: '',
            ym: null
        };

        let dbcon = await this.getConnection();

        try {

            const res = await dbcon.execute(
                'begin ' +
                'ieg_consumer_vols.accept_priem(' +
                ':id_ies_attpoint,' +
                ':do_accept,' +
                ':ym,' +
                ':result_code,' +
                ':result_data' +
                ');' +
                'end;',
                {
                    id_ies_attpoint: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: key },
                    do_accept: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: doAccept ? 0 : 1 },
                    ym: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
                    result_code: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
                    result_data: { type: oracledb.STRING, dir: oracledb.BIND_OUT }
                },
                {
                    resultSet: false,
                    autoCommit: false
                });
            await dbcon.commit();

            ans.code = res.outBinds.result_code;
            ans.msg = res.outBinds.result_data;
            ans.ym = res.outBinds.ym;
        }
        catch (ex) {
            ans.code = -503;
            ans.msg = ex.message;
            ans.ym = null;

            console.error(ex);
            await dbcon.rollback();
            ans.success = false;
        }
        finally {
            this.close(dbcon);
        }

        return ans;
    }

    async set_priem_values(
        prev_reg_key,
        last_reg_key,
        ym,
        p_pu, p_poteri, p_ras, p_dop, p_ob, p_int
    ) {

        const ans = {
            code: 200,
            msg: ''
        };

        let dbcon = await this.getConnection();

        try {
            const res = await dbcon.execute(
                'begin ' +
                'ieg_consumer_vols.set_priem_values(' +
                ':id_ies_prev_ind,' +
                ':id_ies_last_ind,' +
                ':ym,' +
                ':val_pu,' +
                ':val_poteri,' +
                ':val_ras,' +
                ':val_dop,' +
                ':val_ob,' +
                ':val_int,' +
                ':result_code,' +
                ':result_data' +
                ');' +
                'end;',
                {
                    id_ies_prev_ind: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: prev_reg_key },
                    id_ies_last_ind: { type: oracledb.STRING, dir: oracledb.BIND_IN, val: last_reg_key },
                    ym: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: ym },
                    val_pu: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: p_pu },
                    val_poteri: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: p_poteri },
                    val_ras: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: p_ras },
                    val_dop: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: p_dop },
                    val_ob: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: p_ob },
                    val_int: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: p_int },
                    result_code: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
                    result_data: { type: oracledb.STRING, dir: oracledb.BIND_OUT }
                },
                {
                    resultSet: false,
                    autoCommit: false
                });
            await dbcon.commit();

            ans.code = res.outBinds.result_code;
            ans.msg = res.outBinds.result_data;
        }
        catch (ex) {
            ans.code = -503;
            ans.msg = ex.message;

            console.error(ex);
            // await dbcon.rollback();
            await dbcon.commit();
        }

        if (dbcon) {
            try {
                await dbcon.close();
            }
            catch (ex) {
                console.error(ex);
            }
        }
        return ans;
    }


    static ies_data_build_insert(rows) {
        const sql = [];
        sql.push('BEGIN');
        rows.forEach(vals => {
            const parent_id = vals.parent_id ? `'${vals.parent_id}'` : 'NULL';
            const sql_vals = `'${vals.id}', ${parent_id}, '${vals.inf}'`;
            sql.push(`  IEG_MDM.ADD_POINT_IES(${sql_vals});`);
        });
        sql.push('END;');
        return sql.join('\r\n');
    }


    /////////// API
    async queryLog(days, limit) {
        const sql =
            'SELECT LOG_ID, D_M AS DT, MSG_URI, ST AS CODE, ERRTXT AS MSG, ST_OPT AS OPT ' +
            '  FROM IER_LOG ' +
            ' WHERE D_M > SYSDATE - :prm_days ' +
            '   AND ROWNUM <= :prm_rownum ' +
            ' ORDER BY LOG_ID DESC';

        const params = {
            prm_days: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: days },
            prm_rownum: { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: limit }
        }

        return await this.queryWithParams(sql, params);
    }

    async queryIndicat(prm, limit) {
        const params = {};
        params.limit = { type: oracledb.NUMBER, dir: oracledb.BIND_IN, val: limit || 5 };
        const sql = 'SELECT d.NDOG, d.DAT_DOG, o.NAME, pu.NOM_PU, i.KOD_POINT_INI, ind.KOD_INDICAT, ind.COUNTER, o.KOD_NUMOBJ, \n' +
            '    pr.YM, pr.READPREV, pr.READLAST, pr.OUTCOUNTER, pr.OUT, pr.D_M, pr.KOD_PRIEM, d.KOD_DOG \n' +
            '  FROM IER_LINK_OBJECTS lo, NR_INDICAT ind, HR_POINT_INI i, HR_POINT_EN e, HR_POINT_PU pu, HR_POINT p, KR_NUMOBJ o, KR_DOGOVOR d, NR_PRIEM pr \n' +
            ' WHERE 1=1 \n' +
            (prm.id_ies ? `AND lo.ID_IES  = :id_ies \n` : '') +
            '   AND ind.KOD_INDICAT = lo.ID \n' +
            '   AND i.KOD_POINT_INI = ind.KOD_POINT_INI \n' +
            '   AND e.KOD_POINT_EN = i.KOD_POINT_EN \n' +
            '   AND pu.KOD_POINT_PU = e.KOD_POINT_PU \n' +
            '   AND p.KOD_POINT = pu.KOD_POINT \n' +
            '   AND o.KOD_OBJ = p.KOD_OBJ \n' +
            '   AND d.KOD_DOG = o.KOD_DOG \n' +
            '   AND pr.KOD_POINT_INI = i.KOD_POINT_INI \n' +
            '   AND rownum <= :limit \n' +
            ' ORDER BY pr.KOD_PRIEM DESC';

        if (prm.id_ies) {
            params.id_ies = { type: oracledb.STRING, dir: oracledb.BIND_IN, val: prm.id_ies };
        }

        return this.queryWithParams(sql, params);
    }

    async queryWithParams(sql, params) {
        const options = {
            resultSet: false,
            autoCommit: true,
            outFormat: oracledb.OBJECT
        }

        const ans = {}
        try {
            const dbcon = await this.getConnection();
            try {
                const res = await dbcon.execute(sql, params, options);
                ans.data = res.rows;
                ans.meta = res.metaData;
            }
            catch (ex) {
                console.log(sql);
                console.log(params);
                ans.error = ex;
                console.error(ex);
            }
            finally {
                dbcon.close();
            }
        }
        catch (ex) {
            ans.error = ex;
            console.error(ex);
        }
        return ans;
    }

}

module.exports = new DBHelper();