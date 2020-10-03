'use strict';

module.exports = class BaseMsg{
    constructor(data){
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
}