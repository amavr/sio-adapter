'use strict';

const Adapter = require('../../helpers/adapter');
const Register = require('./mdm_register');
const BaseMsg = require('../../framework/base_msg');

module.exports = class MdmCntDevice {

    constructor(node) {
        this.pu_kod_point_pu = Adapter.getVal(node, '@id');
        this.pu_kind = Adapter.getVal(node, 'ИмеетВидИзмерительногоТрансформатора', null);
        this.pu_num = Adapter.getVal(node, 'НомерСредстваИзмерения');
        this.pu_type = Adapter.getVal(node, 'НаименованиеТипаПу');
        this.pu_model = Adapter.getVal(node, 'МодельУстройства');
        this.pu_god_vip = Adapter.getVal(node, 'ДатаИзготовления');
        this.pu_mpi = Adapter.getVal(node, 'МежповерочныйИнтервал');
        this.pu_dat_pp = Adapter.getVal(node, 'ДатаПоследнейПоверки');
        this.pu_dat_s = Adapter.getVal(node, 'ДатаУстановки');
        this.pu_dat_po = Adapter.getVal(node, 'ДатаСнятия');
        this.nodes = Register.parse(node['РегистрНаПу']);
    }

    static getColNames() {
        return [
            ...MdmCntDevice.getSelfColNames(),
            ...Register.getColNames()
        ]
    }

    static getEmpty(owner_data) {
        const rep_data = [...owner_data, ...[null, null, null, null, null, null, null, null, null, null]];
        return Register.getEmpty(rep_data);
    }

    getColValues(owner_data) {
        const my_data = this.getSelfColValues();
        const rep_data = [...owner_data, ...my_data];

        const rows = [];
        /// Если потомков нет, то нужно вернуть только одну строку с пустыми значениями потомка
        if (this.nodes.length === 0) {
            rows.push(Register.getEmpty(rep_data));
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
            'pu_kod_point_pu',
            'pu_num',
            'pu_type',
            'pu_kind',
            'pu_model',
            'pu_mpi',
            'pu_god_vip',
            'pu_dat_pp',
            'pu_dat_s',
            'pu_dat_po'
        ];
    }

    getSelfColValues() {
        return [
            this.pu_kod_point_pu,
            this.pu_num,
            this.pu_type,
            this.pu_kind,
            this.pu_model,
            this.pu_mpi,
            this.pu_god_vip,
            this.pu_dat_pp,
            this.pu_dat_s,
            this.pu_dat_po,
        ];
    }

    /// разбор массива точек поставки
    static parse(nodes) {
        const res = [];
        if (nodes) {
            for (const node of nodes) {
                const devices = node['ПуНаИк'] ? node['ПуНаИк'] : node['ИтНаИк'];
                if(!devices) continue;

                for (const device of devices) {
                    try {
                        res.push(new MdmCntDevice(device));
                    }
                    catch (ex) {
                        BaseMsg.warn(`BAD STRUCTURE FOR DEVICE WITH @ID = ${device['@id']}`);
                        BaseMsg.warn(`${ex.message}`);
                    }
                }
            }
        }
        return res;
    }
}
