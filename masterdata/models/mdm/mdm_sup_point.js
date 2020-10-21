'use strict';

const Adapter = require('../../helpers/adapter');
const CntPoint = require('./mdm_cnt_point');

const ATTP_PWC = 'http://trinidata.ru/sigma/РольЦентрПитания';
const ATTP_SRC = 'http://trinidata.ru/sigma/РольИсточникПитания';
const ATTP_SRCFDR = 'http://trinidata.ru/sigma/РольФидер';


module.exports = class MdmSupPoint {

    constructor(node) {
        this.attp_kod_attpoint = node['@id'];
        // this.attp_kod_attpoint = node['ИспользуетТочкуПрисоединения']['@id'];
        this.attp_num = Adapter.getVal(node, 'НомерТочкиПоставки');
        this.attp_name = Adapter.getVal(node, 'ОписаниеКонтактногоСоединения');
        this.attp_pmax = Adapter.getVal(node, 'МаксимальнаяМощность');
        this.attp_sxpath = Adapter.getVal(node, 'ИспользуетТочкуПрисоединения/ОсуществляетсяПоСхемеПрисоединения/ОписаниеСхемыПрисоединения');

        this.attp_pwc = null;
        this.attp_src = null;
        this.attp_srcfdr = null;
        this.parseNetObjects(node)

        this.attp_voltage_kod = null;
        const voltage_kods = Adapter.getVal(node, 'ИспользуетсяРасчетнаяСхема');
        if(voltage_kods != null && voltage_kods.length > 0){
            this.attp_voltage_kod = Adapter.getVal(voltage_kods[0], 'ИмеетТарифныйУровеньНапряжения');
        }

        // this.attp_pwc = Adapter.getVal(node, 'ИспользуетТочкуПрисоединения/ОСХВСхеме(ИспользуемаяРольОбъектаСетевогоХозяйства)/@id');

        this.nodes = CntPoint.parse(node['ТочкаУчетаРасчетная']);
    }

    static getColNames() {
        return [
            ...MdmSupPoint.getSelfColNames(),
            ...CntPoint.getColNames()
        ]
    }

    static getEmpty(owner_data) {
        const rep_data = [...owner_data, ...[null, null, null, null, null, null, null, null]];
        return CntPoint.getEmpty(rep_data);
    }

    getColValues(owner_data) {
        const my_data = this.getSelfColValues();
        const rep_data = [...owner_data, ...my_data];

        const rows = [];
        /// Если потомков нет, то нужно вернуть только одну строку с пустыми значениями потомка
        if (this.nodes.length === 0) {
            rows.push(CntPoint.getEmpty(rep_data));
        }
        else {
            /// цикл по вложенным объектам
            for (const node of this.nodes) {
                /// каждый потомок возращает массив строк
                for(const row of node.getColValues(rep_data)){
                    rows.push(row);
                }
            }
        }
        return rows;
    }

    static getSelfColNames() {
        return [
            'attp_kod_attpoint',
            'attp_num',
            'attp_name',
            'attp_pmax',
            'attp_sxpath',
            'attp_pwc',
            'attp_src',
            'attp_srcfdr',
            'attp_kod_v'
        ];
    }

    getSelfColValues() {
        return [
            this.attp_kod_attpoint,
            this.attp_num,
            this.attp_name,
            this.attp_pmax,
            this.attp_sxpath,
            this.attp_pwc,
            this.attp_src,
            this.attp_srcfdr,
            this.attp_voltage_kod
        ];
    }

    parseNetObjects(node) {
        const list = Adapter.getVal(node, 'ИспользуетТочкуПрисоединения/ОсуществляетсяПоСхемеПрисоединения/ОСХВСхеме');
        for (var i in list) {
            switch (list[i]['ИспользуемаяРольОбъектаСетевогоХозяйства']) {
                case ATTP_PWC:
                    this.attp_pwc = list[i]['@id'];
                    break;
                case ATTP_SRC:
                    this.attp_src = list[i]['@id'];
                    break;
                case ATTP_SRCFDR:
                    this.attp_srcfdr = list[i]['@id'];
                    break;
            }
        }
    }


    /// разбор массива точек поставки
    static parse(nodes) {
        const res = [];
        if (nodes) {
            for (const node of nodes) {
                try {
                    res.push(new MdmSupPoint(node));
                }
                catch (ex) {
                    console.warn(`BAD STRUCTURE FOR ATTP POINT WITH @ID = ${node['@id']}`);
                    console.warn(ex.message);
                }
            }
        }
        return res;
    }
}
