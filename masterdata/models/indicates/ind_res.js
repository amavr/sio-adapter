'use strict';

module.exports = class IndRes {

    static col_names = ['IND_KOD_INDICAT', 'INI_KOD_POINT_INI', 'IND_DT', 'VAL'];

    constructor(node){
        this.id = node['@id'];
        this.register_id = node['ОтноситсяКРегиструПу'];
        this.dt = node['ДатаВремяПоказаний'];
        this.value = node['ЗначениеПоказаний'];
    }

    static getColNames() {
        return IndDoc.col_names;
    }


    getColValues(owner_data) {
        const data = [this.id, this.register_id, this.dt, this.value];
        return [...owner_data, ...data];
    }


    static parse(nodes) {
        const res = [];
        if (nodes) {
            for (const node of nodes) {
                try {
                    res.push(new IndRes(node));
                }
                catch (ex) {
                    console.warn(ex.message);
                }
            }
        }
        return res;
    }

}