'use strict';

const Utils = require('../../helpers/utils');

module.exports = class VolumeCntPoint {
    constructor(node) {
        this.last_reg_key = this.trimPfx(node['КонечныеПоказания']);
        this.prev_reg_key = this.trimPfx(node['НачальныеПоказания']);
        this.type = this.trimPfx(node['ОбъемВидаПотребления']);
        this.value = node['ОбъемИзмеряемойВеличины'];

        //const x = Utils.extractLastSegment(null);

        /// поля для таблицы ier_msg161
        this.kod = this.trimPfx(node['ОбъемВТочкеУчета']);
        this.rscheme = this.trimPfx(node['ОбъемПоРасчетнойСхеме']);
        this.energy = this.trimPfx(node['ВидЭэУчитываемогоОбъема']);
        this.ym = this.trimPfx(node['ОбъемЗаПериодПотребления']);
        this.volKind = this.trimPfx(node['ОпределяетИзмеряемыйОбъем']);
        this.method = this.trimPfx(node['СпособОпределенияОбъемаТу']);
        this.kodInterval = this.trimPfx(node['ТарифнаяЗонаПотребленнойЭлектроэнергии']);
        this.voltage = this.trimPfx(node['УровеньНапряженияОбъема']);

        this.hash = this.getHash();
    }

    static getColNames() {
        return VolumeCntPoint.getSelfColNames();
    }

    static getSelfColNames() {
        return [
            "PNTBL_KOD",
            "PNT_RSHEMA",
            "INI_ENERGY",
            "INI_READLAST",
            "INI_READPREV",
            "INI_YM",
            "INI_VOLKIND",
            "INI_METHOD",
            "INI_KODINTERVAL",
            "INI_VOLTAGE",

            "INI_VOL_DOP",
            "INI_VOL_INT",
            "INI_VOL_Z",
            "INI_VOL_OB",
            "INI_VOL_PU",
            "INI_VOL_DG",
            "INI_VOL_SR",
            "INI_VOL_POTERI",
            "INI_VOL_RAS",
            "INI_VOL_OTHER"
        ];
    }

    static getEmpty(owner_data) {
        const rep_data = [
            ...owner_data, 
            ...[null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null]];
        return rep_data;
    }



    trimPfx(str){
        return str;
        // return Utils.extractLastSegment(str)
    }

    getKey() {
        return this.last_reg_key;
    }

    getHash() {
        const s = this.last_reg_key 
            + '\t' + this.prev_reg_key 
            + '\t' + this.kod 
            + '\t' + this.rscheme 
            + '\t' + this.energy 
            + '\t' + this.ym
            + '\t' + this.volKind
            + '\t' + this.method
            + '\t' + this.kodInterval
            + '\t' + this.voltage;

        // console.log(s);

        return Utils.getHash(s);
    }

    static hashCode(str) {
        return Array.from(str)
            .reduce((s, c) => Math.imul(31, s) + c.charCodeAt(0) | 0, 0)
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
                items.push(new VolumeCntPoint(n));
            }
        });

        return items;
    }

}