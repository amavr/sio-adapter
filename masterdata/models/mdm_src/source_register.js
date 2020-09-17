'use strict';

const Adapter = require('../../helpers/adapter');

module.exports = class SourceRegister {

    constructor(node) {
        // this.ini_kod_point_ini = Adapter.getVal('@id');
        // this.ini_kod_directen = Adapter.getVal('ИмеетНаправлениеУчета');
        // this.ini_energy = Adapter.getVal('УчитываетВидЭнергии');
        // this.ini_kodinterval = Adapter.getVal('ОтноситсяКТарифнойЗонеСуток');
        // this.ini_rkoef = Adapter.getVal('РасчетныйКоэффициент');
        // this.ini_razr = Adapter.getVal('ЦифрДоРазделителя');
        // this.ini_razr2 = Adapter.getVal('ЦифрПослеРазделителя');

        this.ini_kod_point_ini = node['@id'];
        this.ini_kod_directen = node['ИмеетНаправлениеУчета'];
        this.ini_energy = node['УчитываетВидЭнергии'];
        this.ini_kodinterval = node['ОтноситсяКТарифнойЗонеСуток'];
        this.ini_rkoef = node['РасчетныйКоэффициент'];
        this.ini_razr = node['ЦифрДоРазделителя'];
        this.ini_razr2 = node['ЦифрПослеРазделителя'];        
    }

    static getColNames() {
        return SourceRegister.getSelfColNames();
    }

    static getEmpty(owner_data){
        return [...owner_data, ...[null, null, null, null, null, null, null]];
    }

    getColValues(owner_data){
        const my_data = this.getSelfColValues();
        return [[...owner_data, ...my_data]];
    }

    static getSelfColNames() {
        return [
            'ini_kod_point_ini',
            'ini_kod_directen',
            'ini_energy',
            'ini_kodinterval',
            'ini_rkoef',
            'ini_razr',
            'ini_razr2'
        ];
    }

    getSelfColValues(){
        return [
            this.ini_kod_point_ini,
            this.ini_kod_directen,
            this.ini_energy,
            this.ini_kodinterval,
            this.ini_rkoef,
            this.ini_razr,
            this.ini_razr2
        ];
    }

    /// разбор массива точек поставки
    static parse(nodes) {
        if(nodes === undefined){
            return [];
        }
        
        try {
            return nodes.map(node => new SourceRegister(node))
        }
        catch (ex) {
            console.error(ex);
            return [];
        }
    }
}
