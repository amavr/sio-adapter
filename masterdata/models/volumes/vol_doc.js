'use strict';

const VolumeSupPoint = require("./vol_sup");
const Utils = require('../../helpers/utils');
const BaseMsg = require('../../framework/base_msg');
const CONST = require('../../resources/const.json');

module.exports = class VolumeDoc extends BaseMsg {

    static col_names = null;

    constructor(data) {
        super(data);
        this.tag = '16.1';

        this.pfx = data['@type'].replace(Utils.extractLastSegment(data['@type']), '');
        this.nodes = VolumeSupPoint.parse(data['РассчитанныйОбъемВТочкеПоставки']);
        
        this.flow_type = this.assignFlowType(this.nodes);
    }

    assignFlowType(attp_points){
        /// default value
        if(attp_points === null || attp_points.length === 0) return null;

        const attp_code = attp_points[0].kod_attpoint;

        if(attp_code.includes('ИЖС')){
            this.flow_type = 'ИЖС';
        }
        else if(attp_code.includes('МКД_ЭО_КВ') || attp_code.includes('_ЭО_МКДНС_') || attp_code.includes('_МКДНС_ЭО_КВ_')){
            this.flow_type = 'МКД_КВ';
        }
        else{
            this.flow_type = 'ЮЛ';
        }
    }


    getCounters(){
        const counters = {}
        counters[CONST.RU.msg] = 1;
        counters[CONST.RU.attp] = 0;
        counters[CONST.RU.point] = 0;
        // const counters = {
        //     msg: 1,
        //     attp: 0,
        //     point: 0
        // };

        if(this.nodes){
            counters[CONST.RU.attp] += this.nodes.length;
            this.nodes.forEach((attp_point) => {
                if(attp_point.nodes){
                    counters[CONST.RU.point] += attp_point.nodes.length;
                }
            });
        }

        return counters;
    }

    static getColNames() {
        if (VolumeDoc.col_names === null) {
            VolumeDoc.col_names = [
                ...VolumeDoc.getSelfColNames(),
                ...VolumeSupPoint.getColNames()
            ]
        }
        return VolumeDoc.col_names;
    }

    static getSelfColNames() {
        return [
            'flow_type',
            'filename'
        ];
    }

    getColValues(filename) {
        /**
         * повторяемая часть для всех вложенных объектов
         */
        const my_data = [
            this.flow_type,
            filename,
            ...this.getSelfColValues(),
            // ...this.sup_points.getColValues()
        ];

        /**
         * строки
         */
        const rows = [];
        /// Если потомков нет, то нужно вернуть только одну строку с пустыми значениями потомка
        if (this.nodes.length === 0) {
            rows.push(VolumeSupPoint.getEmpty(rep_data));
        }
        else {
            /// цикл по вложенным объектам
            for (const node of this.nodes) {
                /// каждый потомок возращает массив строк c учетом родительских значений
                for (const row of node.getColValues(my_data)) {
                    /// проверка на значение-массив (да-да и такое встречается!)
                    let err = '';
                    for (const i in row) {
                        const x = row[i];
                        if (x !== null) {
                            if (typeof x === 'object') {
                                if (Array.isArray(x)) {
                                    err += `column #${i} is array`;
                                } else {
                                    err += `column #${i} is object`;
                                }
                                row[i] = null;
                            }
                        }
                    }
                    if (err) {
                        row[1] = err;
                    }
                    rows.push(row);
                }
            }
        }
        return rows;
    }

    getSelfColValues() {
        return [];
    }



    getInserts(filename){
        const lines = [];
        for(const p of this.nodes){
            for(const line of p.getInsertStrings()){
                lines.push(line.replace('##F##', filename));
            }
        }
        return "begin\n" + lines.join(`;\n`) + ";\nend;\n\n";
    }

    getInsertValues(filename){
        const vals = [];
        for(const p of this.nodes){
            for(const row_vals of p.getInsertValues(filename)){
                vals.push(row_vals);
            }
        }
        return vals;
    }

}