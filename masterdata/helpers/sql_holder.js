'use strict';

const path = require('path');
const FileHelper = require('./file_helper');

class SqlHolder {
    constructor(){
        this.sqls = {};
        this.init();
    }

    async init(){
        const dir = './masterdata/helpers/sql/';
        const files = await FileHelper.getFiles(dir);
        for(const file of files){
            const name = path.parse(file).name;
            this.sqls[name] = await FileHelper.read(path.join(dir, file));
        }
    }

    get(name){
        return this.sqls[name];
    }
}

module.exports = new SqlHolder();
