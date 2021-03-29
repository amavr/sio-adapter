'use strict';

const Adapter = require('../../helpers/adapter');

class CalcScheme {

    constructor(node) {
        this.id = node['@id'];

        this.tarif_type = Adapter.getVal(node, 'ТипТарифаРасчетнойСхемы');
        this.transfer_tarif = Adapter.getVal(node, 'ИмеетТарифНаУслугиПоПередаче');
        this.tar_price_group = Adapter.getVal(node, 'ИмеетТарифНаУслугиПоПередаче');
        this.voltage_level = Adapter.getVal(node, 'ИмеетТарифныйУровеньНапряжения');
        this.consumer_categ = Adapter.getVal(node, 'КатегорияПотребителяРасчетнойСхемы');
        this.region = Adapter.getVal(node, 'СубъектРфРасчетнойСхемы');

        this.points = SchemePoint.parse(Adapter.getNodes(node ,'ТочкаУчетаВРасчетнойСхеме'));
    }

    static parse(nodes) {
        const schemes = [];
        for (const node of nodes) {
            const scheme = new CalcScheme(node);
            // схема без точек неинтересна
            if(scheme.points.length > 0){
                schemes.push(scheme);
            }
        }
        return schemes;
    }
}

class SchemePoint {
    constructor(node) {
        const cnt_point = Adapter.getVal(node, 'ЯвляетсяТУ');
        this.id = typeof cnt_point === 'object' ? cnt_point['@id'] : cnt_point;

        this.dt_beg = Adapter.getVal(node, 'ДатаНачала');
        this.dt_end = Adapter.getVal(node, 'ДатаОкончания');
        
        this.type = Adapter.getVal(node, 'ИмеетТипТочкиУчета');
        this.method = Adapter.getVal(node, 'ИмеетМетодРасчета');
        this.scheme = Adapter.getVal(node, 'ИмеетРасчетнуюСхему');
        this.direction = Adapter.getVal(node, 'НаправлениеУчетаРасчетнойСхемыТу');
        this.losses = Adapter.getVal(node, 'ПотериПеременныеВеличина');

        this.month_volumes = SchemePointVolume.parse(Adapter.getVal(node, 'ПомесячныйОбъемДляРасчета', []));
    }

    static parse(nodes) {
        const points = [];
        for (const node of nodes) {
            points.push(new SchemePoint(node));
        }
        return points;
    }
}

class SchemePointVolume {
    constructor(node) {
        this.month_num = parseInt(node['ОпределенДляМесяца']);
        this.value = Adapter.getVal(node, 'ОбъемЗаМесяц');
        this.fixed = Adapter.getVal(node, 'ФиксированныйОбъем');
    }

    static parse(nodes) {
        const volumes = [];
        for (const node of nodes) {
            volumes.push(new SchemePointVolume(node));
        }
        return volumes;
    }
}


module.exports = {
    CalcScheme,
    SchemePoint,
    SchemePointVolume,
};