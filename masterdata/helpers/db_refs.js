'use strict';

const log = require('log4js').getLogger('DBRefs');

const SqlHolder = require('./sql_holder');

module.exports = class DBRefs {
    constructor(){
        this.link_dicts = {
            IndWays: {},
            Direction: {},
            EnergyKind: {},
            Intervals: {}
        }
    }

    async init(db_helper){
        const sql = SqlHolder.get('datadicts');
        const answer = await db_helper.execSql(sql);
        for(const row of answer.data){
            if(row.KOD_DICT === 1){
                this.link_dicts.IndWays[row.ID_IES] = row.ID;
            }
            else if(row.KOD_DICT === 9){
                this.link_dicts.EnergyKind[row.ID_IES] = row.ID;
            }
            else if(row.KOD_DICT === 10){
                this.link_dicts.Direction[row.ID_IES] = row.ID;
            }
            else if(row.KOD_DICT === 11){
                this.link_dicts.Intervals[row.ID_IES] = row.ID;
            }
        }
        log.info(`REFERENCES LOADED`);
    }

}
