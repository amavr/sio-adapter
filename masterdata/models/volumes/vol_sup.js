'use strict';

const VolumeCntPoint = require("./vol_cnt");
const CONST = require("../../resources/const.json");

module.exports = class VolumeSupPoint {

    /*
    , p_pu            in nr_priem.outcounter%type     -- по ПУ = outcounter
    , p_poteri        in nr_priem.potcounter%type     -- Потери = potcounter
    , p_ras           in nr_priem.outadd%type         -- Расчетный = + outadd
    , p_dop           in nr_priem.outadd%type         -- Дополнительный = + outadd
    , p_ob            in number                       -- Общий = + outadd
    , p_int           in number                       -- Интервальный - никуда ???
    */


    constructor(node) {
        this.kod_attpoint = node['ОбъемВТочкеБаланса'];
        this.nodes = VolumeCntPoint.parse(node['РассчитанныйОбъемВТочкеУчета']);
        /// словарь по типам потребления, где в значении - сумма значений для всех элементов с таким ключем
        this.values = this.sumByReadingAndType();
        this.groups = this.groupByReading();

        /// поля для таблицы ier_msg161
        this.energy = node['ВидЭэУчитываемогоОбъема'];
        this.method = node['ИмеетСпособОпределенияОбъема'];
        this.ym = node['ОбъемЗаПериодПотребления'];
        this.vol = node['ОбъемИзмеряемойВеличины'];
        this.rscheme = node['ОбъемПоРасчетнойСхеме'];
        this.volKind = node['ОпределяетИзмеряемыйОбъем'];
        this.chargeKind = node['ОтноситсяКВидуНачисления'];
        this.prBU = node['ПризнакБезучетногоПотребления'] === true ? 'Y' : node['ПризнакБезучетногоПотребления'] === false ? 'N' : 'X';
        this.prAddCharge = node['ПризнакДопначислений'] === true ? 'Y' : node['ПризнакДопначислений'] === false ? 'N' : 'X';
        this.prRecalc = node['ПризнакПерерасчета'] === true ? 'Y' : node['ПризнакПерерасчета'] === false ? 'N' : 'X';
        this.kodInterval = node['ТарифнаяЗонаПотребленнойЭлектроэнергии'];
        this.voltage = node['УровеньНапряженияОбъема'];
    }

    static parse(node) {
        const items = [];
        let nodes = [];
        if (Array.isArray(node)) {
            nodes = node;
        }
        else {
            nodes.push(node);
        }
        nodes.forEach(n => {
            if (n) {
                items.push(new VolumeSupPoint(n));
            }
        });

        return items;
    }


    /// группировка точек учета по их описательной части, значения объемов - элементы группы
    groupByReading() {
        /// словарь по хэшам
        const groups = {};

        for (const item of this.nodes) {
            const hash = item.getHash();
            if (groups[hash] === undefined) {
                groups[hash] = {
                    kod: item.kod,
                    rscheme: item.rscheme,
                    energy: item.energy,
                    last_reg_key: item.last_reg_key,
                    prev_reg_key: item.prev_reg_key,
                    ym: item.ym,
                    volKind: item.volKind,
                    method: item.method,
                    kodInterval: item.kodInterval,
                    voltage: item.voltage,
                    vol_dop: null,
                    vol_int: null,
                    vol_z: null,
                    vol_ob: null,
                    vol_pu: null,
                    vol_dg: null,
                    vol_sr: null,
                    vol_poteri: null,
                    vol_ras: null,
                    vol_other: null
                };
            }

            switch (item.type) {
                case CONST.LOSSES.KEY_PU:
                    groups[hash].vol_pu = item.value;
                    break;
                case CONST.LOSSES.KEY_DOP:
                    groups[hash].vol_dop = item.value;
                    break;
                case CONST.LOSSES.KEY_OB:
                    groups[hash].vol_ob = item.value;
                    break;
                case CONST.LOSSES.KEY_POTERI:
                    groups[hash].vol_poteri = item.value;
                    break;
                case CONST.LOSSES.KEY_RAS:
                    groups[hash].vol_ras = item.value;
                    break;
                case CONST.LOSSES.KEY_INT:
                    groups[hash].vol_int = item.value;
                    break;
                case CONST.LOSSES.KEY_DG:
                    groups[hash].vol_dg = item.value;
                    break;
                case CONST.LOSSES.KEY_SR:
                    groups[hash].vol_sr = item.value;
                    break;
                case CONST.LOSSES.KEY_Z:
                    groups[hash].vol_z = item.value;
                    break;

                default:
                    groups[hash].vol_other = item.value;
            }

        }

        return groups;
    }

    /// суммирование значений по ключу (послед.показания)
    sumByReadingAndType() {
        const dic = {};
        for (const item of this.nodes) {
            const count_point_key = item.getKey();
            const item_type = item.type;

            if (dic[count_point_key] === undefined) {
                dic[count_point_key] = {
                    prev_reg_key: item.prev_reg_key,
                    last_reg_key: item.last_reg_key,
                    readings: {}
                }
            }

            if (dic[count_point_key].readings[item.type] === undefined) {
                dic[count_point_key].readings[item.type] = 0;
            }
            dic[count_point_key].readings[item.type] += item.value;

        }
        return dic;
    }

    static getColNames() {
        return [
            ...VolumeSupPoint.getSelfColNames(),
            ...VolumeCntPoint.getColNames()
        ];
    }

    static getSelfColNames() {
        return [
            "ATTP_KOD_ATTPOINT",
            "ATTP_ENERGY",
            "ATTP_METHOD",
            "ATTP_YM",
            "ATTP_VOL",
            "ATTP_RSHEMA",
            "ATTP_VOLKIND",
            "ATTP_NACHISL",
            "ATTP_PR_BU",
            "ATTP_PR_ADDNACHISL",
            "ATTP_PR_RECALC",
            "ATTP_KODINTERVAL",
            "ATTP_VOLTAGE",
        ];
    }

    static getEmpty(owner_data) {
        const rep_data = [
            ...owner_data,
            ...[null, null, null, null, null, null, null, null, null, null, null, null, null]];
        return VolumeCntPoint.getEmpty(rep_data);
    }


    getColValues(owner_data) {
        const my_data = this.getSelfColValues();
        const rep_data = [...owner_data, ...my_data];

        const rows = [];

        /// Если потомков нет, то нужно вернуть только одну строку с пустыми значениями потомка
        if (this.nodes.length === 0) {
            rows.push(VolumeCntPoint.getEmpty(rep_data));
        }
        else {
            const keys = Object.keys(this.groups);
            if (keys.length > 0) {
                for (const key in this.groups) {
                    const g = this.groups[key];
                    const grp_values = [ 
                        g.kod, g.rscheme,
                        g.energy,g.last_reg_key,g.prev_reg_key,g.ym,g.volKind,g.method,g.kodInterval,g.voltage,
                        g.vol_dop,g.vol_int,g.vol_z,g.vol_ob,g.vol_pu,g.vol_dg,g.vol_sr,g.vol_poteri,g.vol_ras,g.vol_other
                    ];
                    const joined = [...rep_data, ...grp_values];
                    rows.push(joined);
                }
            }
        }
        return rows;
    }

    getSelfColValues() {
        return [
            this.kod_attpoint,
            this.energy,
            this.method,
            this.ym,
            this.vol,
            this.rscheme,
            this.volKind,
            this.chargeKind,
            this.prBU,
            this.prAddCharge,
            this.prRecalc,
            this.kodInterval,
            this.voltage
        ];
    }

    getInsertValues(filename) {
        const rows = [];
        const keys = Object.keys(this.groups);
        if (keys.length > 0) {
            /// общая часть данных от точки поставки
            const supply_values = [
                filename,
                this.kod_attpoint,
                this.energy,
                this.method,
                this.ym,
                this.vol,
                this.rscheme,
                this.volKind,
                this.chargeKind,
                this.prBU,
                this.prAddCharge,
                this.prRecalc,
                this.kodInterval,
                this.voltage
            ];

            const g0 = this.groups[keys[0]];
            /// общая одинаковая часть данных группы от первой точки учета
            const common_counter_values = [
                // g0.energy, 
                // g0.last_reg_key,
                // g0.prev_reg_key,
                // g0.ym,
                // g0.volKind,
                // g0.method,
                // g0.kodInterval,
                // g0.voltage
            ];
            /// переменная часть данных от точки учета (в основном значения)
            for (const key in this.groups) {
                const g = this.groups[key];
                const grp_values = [
                    g.energy,
                    g.last_reg_key,
                    g.prev_reg_key,
                    g.ym,
                    g.volKind,
                    g.method,
                    g.kodInterval,
                    g.voltage,

                    g.kod,
                    g.rscheme,
                    g.vol_dop,
                    g.vol_int,
                    g.vol_z,
                    g.vol_ob,
                    g.vol_pu,
                    g.vol_dg,
                    g.vol_sr,
                    g.vol_poteri,
                    g.vol_ras,
                    g.vol_other
                ];
                const all = [...supply_values, ...common_counter_values, ...grp_values];

                rows.push(all);
            }
        }
        return rows;
    }


    getInsertStrings() {
        const lines = [];
        const keys = Object.keys(this.groups);
        if (keys.length > 0) {
            /// общая часть данных от точки поставки
            const supply_values = `'${this.kod_attpoint}', `
                + `'${this.energy}', `
                + `'${this.method}', `
                + `'${this.ym}', `
                + ` ${this.vol}, `
                + `'${this.rscheme}', `
                + `'${this.volKind}', `
                + `'${this.chargeKind}', `
                + `'${this.prBU}', `
                + `'${this.prAddCharge}', `
                + `'${this.prRecalc}', `
                + `'${this.kodInterval}', `
                + `'${this.voltage}', `;

            const g0 = this.groups[keys[0]];
            /// общая одинаковая часть данных группы от первой точки учета
            const common_counter_values = `'${g0.energy}', `
                + `'${g0.last_reg_key}', `
                + `'${g0.prev_reg_key}', `
                + `'${g0.ym}', `
                + `'${g0.volKind}', `
                + `'${g0.method}', `
                + `'${g0.kodInterval}', `
                + `'${g0.voltage}', `;
            /// переменная часть данных от точки учета (в основном значения)
            for (const key in this.groups) {
                const g = this.groups[key];
                const line = supply_values
                    + common_counter_values
                    + `'${g.kod}', '${g.rscheme}', ${g.vol_dop}, ${g.vol_int}, ${g.vol_z}, ${g.vol_ob}, ${g.vol_pu}, ${g.vol_dg}, ${g.vol_sr}, ${g.vol_poteri}, ${g.vol_ras}, ${g.vol_other}`;

                lines.push(CONST.SQL_INS_161.replace('##V##', line));
            }
        }
        return lines;
    }

    getRowCount() {
        const hashes = [];
        for (const item of this.nodes) {
            const hash = item.getHash();
            if (hashes.includes(hash) === false) {
                hashes.push(hash);
            }
        }
        return hashes.length;
    }

    /// возвращает объект-словарь x[last_val_key] = { volume: summary, prev_key: prev_val_key }
    getSummaryVolumes() {
        /// получение существующих ключей для KEY_CONSUMING
        const ext_ids = this.findKeys();
        const dic = {};

        for (const key of ext_ids) {
            const c = this.nodes.find(point => {
                // return point.last_reg_key === key && point.type === KEY_CONSUMING;
                return point.last_reg_key === key;
            });
            dic[key] = {
                volume: this.getSummary(key),
                prev_key: c.prev_reg_key
            }
        }

        return dic;
    }

    findKeys() {
        return this.nodes
            .map(point => point.last_reg_key)
            .filter((val, index, self) => self.indexOf(val) === index);
    }

    getSummary(ext_id) {
        /// свертка, значение для ТУ с типом KEY_CONSUMING не учитывается
        // const reducer = (accum, point) => accum + (point.type === KEY_CONSUMING ? 0 : point.value);
        const reducer = (accum, point) => {
            return accum + (point.type === CONST.KEY_PU ? 0 : point.value)
        };

        return this.nodes
            /// отфильтрованные по ключу
            .filter(p => p.last_reg_key === ext_id)
            /// просуммированные
            .reduce(reducer, 0);
    }
}