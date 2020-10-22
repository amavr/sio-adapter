'use strict';

const Adapter = require('../../helpers/adapter');
const MdmRegister = require('../mdm/mdm_register');
const IndRes = require('../indicates/ind_res');

module.exports = class CfgPu {

    constructor(data) {
        this.id = data['@id'];
        this.issue_year = parseInt(data['ДатаИзготовления']);
        this.dt_check = new Date(Date.parse(data['ДатаПоследнейПоверки']));
        this.dt_install = new Date(Date.parse(data['ДатаУстановки']));
        this.dt_uninstall = new Date(Date.parse(data['ДатаСнятия']));
        // this.dt_check = data['ДатаПоследнейПоверки'];
        // this.dt_install = data['ДатаУстановки'];
        // this.dt_uninstall = data['ДатаСнятия'];
        this.model = data['МодельУстройства'];
        this.mpi = data['МежповерочныйИнтервал'];
        this.num = data['НомерСредстваИзмерения'];

        const reg_data = Adapter.getVal(data, 'РегистрНаПу');
        this.registers = MdmRegister.parse(reg_data);

        const ind_route = data['ПоказанияСнятияПу'] ? 'ПоказанияСнятияПу' : 'ПоказанияУстановкиПу';
        this.indicates_way = Adapter.getVal(data, `${ind_route}/ИмеетСпособПолученияПоказаний`);
        this.indicates_dt = Adapter.getVal(data, `${ind_route}/ДатаСнятияПоказаний`);
        const ind_data = Adapter.getVal(data, `${ind_route}/СнятыеПоказанияРегистра`);
        this.indictates = IndRes.parse(ind_data);

        this.linkIndicates();
    }

    linkIndicates(){
        /// чтобы не искать несколькими проходами
        const dic = {};
        for(const reg of this.registers){
            dic[reg.ini_kod_point_ini] = reg;
        }

        /// установка показаний у шкалы 
        for(const ind of this.indictates){
            if(dic[ind.register_id]){
                dic[ind.register_id].ind = ind;
            }
        }
    }

    static parse(nodes) {
        const res = [];
        if (nodes) {
            for (const node of nodes) {
                const node_id = node['@id'];
                try {
                    res.push(new CfgPu(node));
                }
                catch (ex) {
                    console.warn(`BAD STRUCTURE: node "ПуНаИк" not valid @id = ${node_id}`);
                    console.warn(ex.message);
                }
            }
        }
        return res;
    }
}
