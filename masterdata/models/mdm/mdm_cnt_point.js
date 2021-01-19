'use strict';

const Adapter = require('../../helpers/adapter');
const CntDevice = require('./mdm_cnt_device');
const BaseMsg = require('../../framework/base_msg');

module.exports = class MdmCntPoint {

    constructor(node) {
        this.pnt_kod_point = node['@id'];
        this.pnt_num = Adapter.getVal(node, 'НомерТочкиУчета');
        this.pnt_name = Adapter.getVal(node, 'НаименованиеТу');
        this.pnt_dat_s = Adapter.getVal(node, 'ДатаНачалаДействияТУ');
        this.pnt_dat_po = Adapter.getVal(node, 'ДатаОкончанияДействияТУ');
        this.pnt_transit = Adapter.getVal(node, 'ЯвляетсяТранзитной') === true ? 1 : 0;
        this.pnt_rs_props = {}; // информация из расчетной схемы объекта
        this.nodes = CntDevice.parse(node['ИзмерительныйКомплексНаТу']);
    }

    static getColNames() {
        return [
            ...MdmCntPoint.getSelfColNames(),
            ...CntDevice.getColNames()
        ]
    }

    static getEmpty(owner_data) {
        const rep_data = [...owner_data, ...[null, null, null, null, null, null, null, null, null, null, null]];
        return CntDevice.getEmpty(rep_data);
    }

    getColValues(owner_data) {
        const my_data = this.getSelfColValues();
        const rep_data = [...owner_data, ...my_data];

        const rows = [];
        /// Если потомков нет, то нужно вернуть только одну строку с пустыми значениями потомка
        if (this.nodes.length === 0) {
            rows.push(CntDevice.getEmpty(rep_data));
        }
        else {
            /// цикл по вложенным объектам
            for (const node of this.nodes) {
                /// каждый потомок возращает массив строк
                for (const row of node.getColValues(rep_data)) {
                    rows.push(row);
                }
            }
        }
        return rows;
    }

    static getSelfColNames() {
        return [
            'pnt_kod_point',
            'pnt_num',
            'pnt_name',
            'pnt_dat_s',
            'pnt_dat_po',
            'pnt_transit',
            'pnt_calc_method',
            'pnt_tar_pricegroup',
            'pnt_tar_voltage',
            'pnt_tar_consgroup',
            'pnt_tar_region'
        ];
    }

    getSelfColValues() {
        return [
            this.pnt_kod_point,
            this.pnt_num,
            this.pnt_name,
            this.pnt_dat_s,
            this.pnt_dat_po,
            this.pnt_transit,
            this.pnt_rs_props.calc_method,
            this.pnt_rs_props.tar_price_group,
            this.pnt_rs_props.tar_voltage,
            this.pnt_rs_props.tar_cons_group,
            this.pnt_rs_props.tar_region,
        ];
    }

    /// разбор массива точек поставки
    static parse(nodes) {
        const res = [];
        if (nodes) {
            for (const node of nodes) {
                try {
                    res.push(new MdmCntPoint(node));
                }
                catch (ex) {
                    BaseMsg.warn(`BAD STRUCTURE FOR POINT WITH @ID = ${node['@id']}`);
                    BaseMsg.warn(ex.message);
                }
            }
        }
        return res;
    }
}
