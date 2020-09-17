'use strict';

const Result = require('./result');

module.exports = class Indicat {

    constructor(){
        this.id = null;
        this.device_id = null;
        this.type = null;
        this.dt = null;
        this.results = [];
    }

    static parse(node) {
        const ind = new Indicat();
        ind.id = node['@id'];
        ind.device_id = node['ПуПоказаний'];
        ind.type = node['ИмеетСпособПолученияПоказаний'];
        ind.dt = new Date(node['ДатаСнятияПоказаний']);
        ind.results = Result.parse(node['СнятыеПоказанияРегистра']);
        return ind;
    }



}