'use strict';

const CONST = require('../../resources/const.json');
const Adapter = require('../../helpers/adapter');
const BaseMsg = require('../../framework/base_msg');
const {CalcScheme } = require('../md_common/calc_scheme');

module.exports = class Mdm721 extends BaseMsg {

    static col_names = null;

    constructor(data) {
        super(data);

        this.id = data['@id'];
        this.dt = Date.parse(date['ДатаСобытия']);
        this.schemes = CalcScheme.parse(Adapter.getNodes(data, 'ОбеспечиваетсяЭэЧерезТочкиПоставки/ИспользуетсяРасчетнаяСхема'));
    }

    static parse(data){
        const data = JSON.parse(msg.data);
        Adapter.normalize(data, '', CONST.ARRAY_ROUTES.map(item => item.toLowerCase()));
        return new Mdm721(data);
    }

}