'use strict';

const Adapter = require('../../helpers/adapter');

const IndRes = require('./ind_res');
const BaseMsg = require('../../framework/base_msg');
const CONST = require('../../resources/const.json');

module.exports = class IndDoc extends BaseMsg {

    static col_names = ['FILENAME', 'PACK_IES', 'PU_KOD_POINT_PU', 'POKTYPE_IES', 'READ_DT'];

    constructor(data){
        super(data);

        Adapter.normalize(data, '',
            [
                '/СнятыеПоказанияРегистра',
            ].map(item => item.toLowerCase()));


        this.tag = '13.1';

        this.ind_id = data['@id'];
        this.device_id = data['ПуПоказаний'];
        this.type = data['ИмеетСпособПолученияПоказаний'];
        this.dt = data['ДатаСнятияПоказаний'];

        this.nodes = IndRes.parse(data['СнятыеПоказанияРегистра']);
    }

    getColValues(filename) {
        const data = [filename, this.ind_id, this.device_id, this.type, this.dt];
        const rows = [];
        for(const node of this.nodes){
            if(node){
                rows.push(node.getColValues(data));
            }
        }
        return rows;
    }

    getCounters(){
        const counters = {}
        counters[CONST.RU.msg] = 1;
        counters[CONST.RU.ind] = 0;

        if(this.nodes){
            counters[CONST.RU.ini] = this.nodes.length;
        }

        return counters;
    }

    static getColNames() {
        return [...IndDoc.col_names, ...IndRes.col_names];
    }

    static parse(nodes) {
        const res = [];
        if (nodes) {
            for (const node of nodes) {
                try {
                    res.push(new IndDoc(node));
                }
                catch (ex) {
                    console.warn(ex.message);
                }
            }
        }
        return res;
    }
}