'use strict';

const Adapter = require('../../helpers/adapter');

class CalcScheme {

    constructor(node) {
        this.id = node['@id'];

        this.tarif_type = node['ТипТарифаРасчетнойСхемы'];
        this.transfer_tarif = node['ИмеетТарифНаУслугиПоПередаче'];
        this.tar_price_group = node['ИмеетТарифНаУслугиПоПередаче'];
        this.voltage_level = node['ИмеетТарифныйУровеньНапряжения'];
        this.consumer_categ = node['КатегорияПотребителяРасчетнойСхемы'];
        this.region = node['СубъектРфРасчетнойСхемы'];

        this.points = SchemePoint.parse(Adapter.getVal(node ,'ТочкаУчетаВРасчетнойСхеме', []));
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

        this.type = node['ИмеетТипТочкиУчета'];
        this.method = node['ИмеетМетодРасчета'];
        this.direction = node['НаправлениеУчетаРасчетнойСхемыТу'];
        this.losses = node['ПотериПеременныеВеличина'];

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
        this.value = node['ОбъемЗаМесяц'];
        this.fixed = node['ФиксированныйОбъем'];
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