'use strict';

const Result = require('./result');
const BaseMsg = require('../../framework/base_msg');

module.exports = class Indicat extends BaseMsg {

    constructor(data){
        super(data);

        this.ind_id = data['@id'];
        this.device_id = data['ПуПоказаний'];
        this.type = data['ИмеетСпособПолученияПоказаний'];
        this.dt = new Date(data['ДатаСнятияПоказаний']);
        this.nodes = Result.parse(data['СнятыеПоказанияРегистра']);
    }

    getCounters(){
        const counters = {
            ind: 1,
            ini: 0
        };

        if(this.nodes){
            counters.ini = this.nodes.length;
        }

        return counters;
    }

    static parse(data) {
        return new Indicat(data);
    }

}