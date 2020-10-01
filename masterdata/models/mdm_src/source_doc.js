'use strict';

const Adapter = require('../../helpers/adapter');
const SupPoint = require('./source_sup_point');
const Addr = require('./source_addr');
const { CQN_OPCODE_ALL_OPS } = require('oracledb');
const SourceSupPoint = require('./source_sup_point');


const MSG61_TAB = 'SIO_MSG6_1';

module.exports = class SourceDoc {

    static col_names = null;

    constructor(data) {
        Adapter.normalize(data, '',
            [
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки',
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки/ИспользуетсяРасчетнаяСхема',
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки/ТочкаУчетаРасчетная',
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки/ТочкаУчетаРасчетная/ИзмерительныйКомплексНаТу/ПуНаИк',
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки/ТочкаУчетаРасчетная/ИзмерительныйКомплексНаТу/ПуНаИк/РегистрНаПу'
            ].map(item => item.toLowerCase()));

        this.abon_kodp = Adapter.getVal(data, 'СнабжаетсяНаОсновеДоговора/ЯвляетсяПотребителем/@id');
        this.abon_name = Adapter.getVal(data, 'СнабжаетсяНаОсновеДоговора/ЯвляетсяПотребителем/НаименованиеПолное');
        this.abon_inn = Adapter.getVal(data, 'СнабжаетсяНаОсновеДоговора/ЯвляетсяПотребителем/Инн');
        this.abon_kpp = Adapter.getVal(data, 'СнабжаетсяНаОсновеДоговора/ЯвляетсяПотребителем/Кпп');
        this.is_physical = Adapter.getVal(data, 'СнабжаетсяНаОсновеДоговора/ПризнакБытовогоПотребителя') === true ? 'Y' : 'N';
        this.flow_type = null; // will assigned bellow

        this.abon_adr_r = Addr.parse(Adapter.getVal(data, 'СнабжаетсяНаОсновеДоговора/ЯвляетсяПотребителем/ИмеетАдресРегистрации'));
        this.abon_adr_f = Addr.parse(Adapter.getVal(data, 'СнабжаетсяНаОсновеДоговора/ЯвляетсяПотребителем/ИмеетАдресФактический'));
        this.nobj_adr = Addr.parse(Adapter.getVal(data, 'ИмеетАдресФактический'));

        // this.abon_adr_r = data['СнабжаетсяНаОсновеДоговора']['ЯвляетсяПотребителем']['ИмеетАдресРегистрации']['АдресПолныйСтрокой'];
        // this.abon_adr_f = data['СнабжаетсяНаОсновеДоговора']['ЯвляетсяПотребителем']['ИмеетАдресФактический']['АдресПолныйСтрокой'];
        this.dg_kod_dog = Adapter.getVal(data, 'СнабжаетсяНаОсновеДоговора/@id');
        this.dg_ndog = Adapter.getVal(data, 'СнабжаетсяНаОсновеДоговора/АктуальныйНомерДоговора');
        this.dg_kind = Adapter.getVal(data, 'СнабжаетсяНаОсновеДоговора/ОтнесенККлассуПотребителей');
        this.dg_dat_numdog = Adapter.getVal(data, 'СнабжаетсяНаОсновеДоговора/ДатаДокумента');
        this.dg_dat_dog = Adapter.getVal(data, 'СнабжаетсяНаОсновеДоговора/ДатаВступленияВСилуДокумента');
        this.dg_dat_fin = Adapter.getVal(data, 'СнабжаетсяНаОсновеДоговора/ДатаОкончанияДействияДокумента');
        if(Array.isArray(this.dg_dat_fin)){
            this.dg_dat_fin = this.dg_dat_fin.pop();
        }
        this.dg_dep = 'Упр';
        this.nobj_kod_numobj = Adapter.getVal(data, '@id');
        this.nobj_num = Adapter.getVal(data, 'НомерОбъектаЭнергоснабжения');
        this.nobj_name = Adapter.getVal(data, 'НаименованиеЭнергообъекта');
        this.nobj_dat_create = Adapter.getVal(data, 'ДатаНачалаАктивности');
        this.nobj_dat_fin = Adapter.getVal(data, 'ДатаЗавершенияАктивности');
        this.nobj_kat = Adapter.getVal(data, 'КатегорияНадежности');

        const ds = Adapter.getVal(data, 'ПризнакИсусэБп', null);
        this.nobj_datasource = ds === true 
            ? 'ПЭС' : ds === false 
            ? 'ЕБ' : null;

        const attp_points = Adapter.getVal(data, 'ОбеспечиваетсяЭэЧерезТочкиПоставки');
        this.nodes = attp_points === null ? null : SupPoint.parse(attp_points);
        
        this.assignFlowType(attp_points);
    }

    getCounters(){
        const counters = {
            abon: 1,
            dog: 1,
            obj: 1,
            attp: 0,
            point: 0,
            pu: 0,
            ini: 0
        };

        /// ТП
        if(this.nodes){
            counters.attp = this.nodes.length;
            this.nodes.forEach((attp_node) => {
                /// ТУ
                if(attp_node.nodes){
                    counters.point += attp_node.nodes.length;
                    attp_node.nodes.forEach((point_node) => {
                        /// ПУ
                        if(point_node.nodes){
                            counters.pu += point_node.nodes.length;
                            point_node.nodes.forEach((pu_node) => {
                                /// Шкалы
                                if(pu_node.nodes){
                                    counters.ini += pu_node.nodes.length;
                                }
                            });
                        }
                    });
                }
            });
        }

        return counters;
    }

    assignFlowType(attp_points){
        /// default value
        this.flow_type = 'ЮЛ';

        if(attp_points === null || attp_points.length === 0) return;

        const obj_id = attp_points[0]['@id'];

        if(obj_id.includes('ИЖС')){
            this.flow_type = 'ИЖС';
        }
        else if(obj_id.includes('МКД_ЭО_КВ')){
            this.flow_type = 'МКД_КВ';
        }
        else{
            this.flow_type = 'ЮЛ';
        }
    }


    /**
     * Получить все наименования столбцов: свои столбцы + столбцы инкапсулированных классов
     */
    static getColNames() {
        if (SourceDoc.col_names === null) {
            SourceDoc.col_names = [
                ...SourceDoc.getSelfColNames(),
                ...Addr.getColNames('abon_adrr'),
                ...Addr.getColNames('abon_adrf'),
                ...Addr.getColNames('nobj_adr'),
                ...SourceSupPoint.getColNames()
            ]
        }
        return SourceDoc.col_names;
    }

    static getSelfColNames() {
        return [
            'filename',
            'abon_kodp',
            'abon_name',
            'abon_inn',
            'abon_kpp',
            'is_physical',
            'flow_type',
            'dg_kod_dog',
            'dg_ndog',
            'dg_kind',
            'dg_dat_numdog',
            'dg_dat_dog',
            'dg_dat_fin',
            'dg_dep',
            'nobj_kod_numobj',
            'nobj_datasource',
            'nobj_num',
            'nobj_name',
            'nobj_dat_create',
            'nobj_dat_fin',
            'nobj_kat'
        ];
    }

    /**
     * Получить данные для записи в БД, 
     * т.к. таблица плоская - вложенные объекты дают перемножение кол-ва строк
     * 
     * @return массив масивов (значения столбцов в строках)
     */
    getColValues(filename) {
        /**
         * повторяемая часть для всех вложенных объектов
         */
        const my_data = [
            filename,
            ...this.getSelfColValues(),
            ...this.abon_adr_r.getColValues(),
            ...this.abon_adr_f.getColValues(),
            ...this.nobj_adr.getColValues()
        ];

        /**
         * строки
         */
        const rows = [];
        /// Если потомков нет, то нужно вернуть только одну строку с пустыми значениями потомка
        if (this.nodes.length === 0) {
            rows.push(SupPoint.getEmpty(rep_data));
        }
        else {
            /// цикл по вложенным объектам
            for (const node of this.nodes) {
                /// каждый потомок возращает массив строк c учетом родительских значений
                for (const row of node.getColValues(my_data)) {
                    /// проверка на значение-массив (да-да и такое встречается!)
                    let err = '';
                    for (const i in row) {
                        const x = row[i];
                        if (x !== null) {
                            if (typeof x === 'object') {
                                if (Array.isArray(x)) {
                                    err += `column #${i} is array`;
                                } else {
                                    err += `column #${i} is object`;
                                }
                                row[i] = null;
                            }
                        }
                    }
                    if (err) {
                        row[1] = err;
                    }
                    rows.push(row);
                }
            }
        }
        return rows;
    }

    getSelfColValues() {
        return [
            this.abon_kodp,
            this.abon_name,
            this.abon_inn,
            this.abon_kpp,
            this.is_physical,
            this.flow_type,
            this.dg_kod_dog,
            this.dg_ndog,
            this.dg_kind,
            this.dg_dat_numdog,
            this.dg_dat_dog,
            this.dg_dat_fin,
            this.dg_dep,
            this.nobj_kod_numobj,
            this.nobj_datasource,
            this.nobj_num,
            this.nobj_name,
            this.nobj_dat_create,
            this.nobj_dat_fin,
            this.nobj_kat
        ];
    }


    generateInserts(file) {
        const fields = Adapter.getFields(this);
        // console.log(JSON.stringify(fields));

        const inserts = this.prepareInserts(fields).map(x => {
            return '' +
                `INSERT INTO ${MSG61_TAB}(` + x.names.substr(1) + ', filename) ' +
                'VALUES(' + x.vals.substr(1) + `,'${file}');`;
        });

        // console.log(JSON.stringify(inserts));
        return inserts;
    }

    getBase(base) {
        return base.reduce((res, item) => {
            res.names += `,${item.name}`;
            res.vals += item.val !== null ? `,'${item.val}'` : ',null';
            return res;
        }, { names: '', vals: '' });
    }

    prepareInserts(data) {
        const base_part = this.getBase(data.base);
        // console.log(base_part.names);

        if (data.kids && data.kids.length > 0) {
            let kid_parts = [];
            data.kids.forEach(item => {
                const kid_part = this.prepareInserts(item);
                for (let i in kid_part) {
                    kid_part[i].names = base_part.names + kid_part[i].names;
                    kid_part[i].vals = base_part.vals + kid_part[i].vals;
                }
                kid_parts = kid_parts.concat(kid_part);
            });
            return kid_parts;
        }
        return [base_part];
    }
}