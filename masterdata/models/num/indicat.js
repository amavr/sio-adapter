'use strict';

const Result = require('./result');

module.exports = class Indicat {

    constructor(){
        this.id = null;
        this.device_id = null;
        this.type = null;
        this.dt = null;
        this.nodes = [];
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

    static parse(node) {
        const ind = new Indicat();
        ind.id = node['@id'];
        ind.device_id = node['ПуПоказаний'];
        ind.type = node['ИмеетСпособПолученияПоказаний'];
        ind.dt = new Date(node['ДатаСнятияПоказаний']);
        ind.nodes = Result.parse(node['СнятыеПоказанияРегистра']);
        return ind;
    }

}