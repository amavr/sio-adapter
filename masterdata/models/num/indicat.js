'use strict';

const Result = require('./result');
const BaseMsg = require('../../framework/base_msg');
const CONST = require('../../resources/const.json');

module.exports = class Indicat extends BaseMsg {

    constructor(data){
        super(data);
        this.tag = '13.1';

        this.ind_id = data['@id'];
        this.device_id = data['ПуПоказаний'];
        this.type = data['ИмеетСпособПолученияПоказаний'];
        this.dt = new Date(data['ДатаСнятияПоказаний']);
        this.nodes = Result.parse(data['СнятыеПоказанияРегистра']);
    }

    getCounters(){
        const counters = {}
        counters[CONST.RU.msg] = 1;
        counters[CONST.RU.ind] = 0;

        // const counters = {
        //     msg: 1,
        //     ind: 0
        // };

        if(this.nodes){
            counters[CONST.RU.ini] = this.nodes.length;
        }

        return counters;
    }

    static parse(data) {
        return new Indicat(data);
    }

}