'use strict';

const Adapter = require('../../helpers/adapter');

const IndRes = require('./ind_res');
const BaseMsg = require('../../framework/base_msg');
const CONST = require('../../resources/const.json');

module.exports = class IndDoc extends BaseMsg {

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

    getCounters(){
        const counters = {}
        counters[CONST.RU.msg] = 1;
        counters[CONST.RU.ind] = 0;

        if(this.nodes){
            counters[CONST.RU.ini] = this.nodes.length;
        }

        return counters;
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