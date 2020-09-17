'use strict';

const VolumeSupPoint = require("./vol_sup");
const Utils = require('../../helpers/utils');

module.exports = class VolumeDoc {

    static col_names = null;

    constructor(data) {
        this.pfx = data['@type'].replace(Utils.extractLastSegment(data['@type']), '');
        this.sup_points = VolumeSupPoint.parse(data['РассчитанныйОбъемВТочкеПоставки']);
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
            'filename'
        ];
    }

    getColValues(filename) {
        /**
         * повторяемая часть для всех вложенных объектов
         */
        const my_data = [
            filename,
            ...this.getSelfColValues(),
            // ...this.sup_points.getColValues()
        ];

        /**
         * строки
         */
        const rows = [];
        /// Если потомков нет, то нужно вернуть только одну строку с пустыми значениями потомка
        if (this.sup_points.length === 0) {
            rows.push(VolumeSupPoint.getEmpty(rep_data));
        }
        else {
            /// цикл по вложенным объектам
            for (const node of this.sup_points) {
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
        for(const p of this.sup_points){
            for(const line of p.getInsertStrings()){
                lines.push(line.replace('##F##', filename));
            }
        }
        return "begin\n" + lines.join(`;\n`) + ";\nend;\n\n";
    }

    getInsertValues(filename){
        const vals = [];
        for(const p of this.sup_points){
            for(const row_vals of p.getInsertValues(filename)){
                vals.push(row_vals);
            }
        }
        return vals;
    }

}