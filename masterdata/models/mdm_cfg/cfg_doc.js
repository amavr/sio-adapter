'use strict';

const Adapter = require('../../helpers/adapter');
const BaseMsg = require('../../framework/base_msg');

const CfgPu = require('./cfg_pu');

module.exports = class CfgDoc extends BaseMsg {

    constructor(data) {
        super(data);

        Adapter.normalize(data, '',
            [
                '/ПуНаИк',
                '/ПуНаИк/РегистрНаПу',
                '/ПуНаИк/ПоказанияУстановкиПу/СнятыеПоказанияРегистра',
                '/ПуНаИк/ПоказанияСнятияПу/СнятыеПоказанияРегистра',
            ].map(item => item.toLowerCase()));

        const val = Adapter.getVal(data, 'НаходитсяВЭксплуатации');
        this.tag = val  === null ? '5.x' : val ? '5.1' : '5.5';

        this.id = data['@id'];
        this.dt_beg = data['ДатаНачала'];
        this.dt_end = data['ДатаОкончания'];
        this.pnt_kod_point = data['СвязанСТочкойУчета'];

        this.nodes = CfgPu.parse(data['ПуНаИк']);
    }
   

}
