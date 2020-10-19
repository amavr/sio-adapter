'use strict';

const oracledb = require('oracledb');
const log = require('log4js').getLogger('DBHelper');
const Utils = require('../../helpers/utils');

module.exports = class OraAdapter {
    constructor(){
        this.pool = null;
    }

    init(pool){
        this.pool = pool;
    }

    async handle61(doc){

    }
}