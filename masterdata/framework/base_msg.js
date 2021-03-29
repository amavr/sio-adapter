'use strict';

module.exports = class BaseMsg{

    static get log() { return this._log; }
    static set log(l) { this._log = l; }

    constructor(data){
        this.errors = [];
        this.id = data.id;
        this.tag = 'SYS';
        this.counters = null;
        this.colNames = null;
        this.colValues = null;
    }

    getCountersData(){
        if(this.counters === null){
            this.counters = this.getCounters();
        }
        return this.counters;
    }

    getColNamesData(){
        if(this.colNames === null){
            this.colNames = this.getColNames();
        }
        return this.colNames;
    }

    getColValuesData(){
        if(this.colValues === null){
            this.colValues = this.getColValues();
        }
        return this.countcolValuesers;
    }

    getCounters(){
        return {};
    }

    getColNames(){
        return [];
    }

    getColValues(){
        return [];
    }

    static info(msg){
        if(this.log){
            this.log.info(msg);
        }
    }

    static warn(msg){
        if(this.log){
            this.log.warn(msg);
        }
    }

    static error(msg){
        if(this.log){
            this.log.error(msg);
        }
    }
}