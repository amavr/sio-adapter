'use strict';

module.exports = class IndRes {

    constructor(node){
        this.id = node['@id'];
        this.register_id = node['ОтноситсяКРегиструПу'];
        this.dt = node['ДатаВремяПоказаний'];
        this.value = node['ЗначениеПоказаний'];
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