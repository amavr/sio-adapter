'use strict';

const Adapter = require('../../helpers/adapter');
const SupPoint = require('./mdm_sup_point');
const Addr = require('./mdm_addr');
const MdmSupPoint = require('./mdm_sup_point');
const BaseMsg = require('../../framework/base_msg');
const CONST = require('../../resources/const.json');


const MSG61_TAB = 'SIO_MSG6_1';

module.exports = class MdmDoc extends BaseMsg {

    static col_names = null;

    constructor(data) {
        super(data);
        this.tag = '6.1';
        this.errors = [];
        MdmDoc.getColNames();

        Adapter.normalize(data, '', CONST.ARRAY_ROUTES.map(item => item.toLowerCase()));

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
        if (Array.isArray(this.dg_dat_fin)) {
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

        this.transit = null;        // иерархия и характеристики ТУ
        this.balance_points = []; // соответствие точки баланса (ТБ) точке учета ТУ - используется в обработке 16.1
        this.calc_schema = {
            points: {} // словарь точек (ключ - ID точки, значение - объект (метод расч., ...) )
        };
        this.extractTransitChains(data);
        this.setPointsProps();
    }

    getCounters() {
        const counters = {};
        counters[CONST.RU.msg] = 1;
        counters[CONST.RU.abon] = 1;
        counters[CONST.RU.dog] = 1;
        counters[CONST.RU.obj] = 1;
        counters[CONST.RU.attp] = 0;
        counters[CONST.RU.point] = 0;
        counters[CONST.RU.pu] = 0;
        counters[CONST.RU.ini] = 0;
        // const counters = {
        //     abon: 1,
        //     dog: 1,
        //     obj: 1,
        //     attp: 0,
        //     point: 0,
        //     pu: 0,
        //     ini: 0
        // };

        /// ТП
        if (this.nodes) {
            counters[CONST.RU.attp] = this.nodes.length;
            this.nodes.forEach((attp_node) => {
                /// ТУ
                if (attp_node.nodes) {
                    counters[CONST.RU.point] += attp_node.nodes.length;
                    attp_node.nodes.forEach((point_node) => {
                        /// ПУ
                        if (point_node.nodes) {
                            counters[CONST.RU.pu] += point_node.nodes.length;
                            point_node.nodes.forEach((pu_node) => {
                                /// Шкалы
                                if (pu_node.nodes) {
                                    counters[CONST.RU.ini] += pu_node.nodes.length;
                                }
                            });
                        }
                    });
                }
            });
        }

        return counters;
    }

    assignFlowType(attp_points) {
        /// default value
        this.flow_type = 'ЮЛ';

        if (attp_points === null || attp_points.length === 0) return;

        const obj_id = attp_points[0]['@id'];

        if (obj_id.includes('ИЖС')) {
            this.flow_type = 'ИЖС';
        }
        else if (obj_id.includes('МКД_ЭО_КВ') || obj_id.includes('_ЭО_МКДНС_') || obj_id.includes('_МКДНС_ЭО_КВ_')) {
            this.flow_type = 'МКД_КВ';
        }
        else {
            this.flow_type = 'ЮЛ';
        }
    }

    extractTransitChains(data) {
        this.balance_points.length = 0; // очистка массива связей ТБ и ТУ
        const chains = [];
        
        // словарь ссылок на ТУ для назначения им тарифных свойств из РасчСхемы
        const points_dic = this.getCntPoints();

        // Обход точек подключения
        const attp_points = Adapter.getVal(data, 'ОбеспечиваетсяЭэЧерезТочкиПоставки');
        if (attp_points === null) return chains;

        const schemas = {};
        for (const ap of attp_points) {

            // перебор всех СР и транзитных цепочек
            const used_schemas = Adapter.getVal(ap, 'ИспользуетсяРасчетнаяСхема');
            if (used_schemas === null) continue;

            for (const sch of used_schemas) {
                const schema_id = sch['@id'];
                schemas[schema_id] = { parents: [], childs: [] }
                const schema_points = Adapter.getVal(sch, 'ТочкаУчетаВРасчетнойСхеме');
                if (schema_points === null) continue;
                if (schema_points.length === 0) continue;

                // перебор точек СР
                for (const p of schema_points) {
                    const ptype = p['ИмеетТипТочкиУчета'];
                    const method = p['ИмеетМетодРасчета'];
                    const cnt_point = Adapter.getVal(p, 'ЯвляетсяТУ');
                    if (cnt_point === null) continue;

                    const pid = typeof cnt_point === 'object' ? cnt_point['@id'] : cnt_point;

                    if(points_dic[pid] !== undefined){
                        points_dic[pid].pnt_rs_props.tar_price_group = sch['ИмеетТарифНаУслугиПоПередаче'];
                        points_dic[pid].pnt_rs_props.tar_voltage = sch['ИмеетТарифныйУровеньНапряжения'];
                        points_dic[pid].pnt_rs_props.tar_cons_group = sch['КатегорияПотребителяРасчетнойСхемы'];
                        points_dic[pid].pnt_rs_props.tar_region = sch['СубъектРфРасчетнойСхемы'];
                    }

                    if (this.calc_schema.points[pid] === undefined) {
                        this.calc_schema.points[pid] = {
                            calc_method: method
                        }
                    }

                    const balance_point = p['@id'];
                    this.balance_points.push([balance_point, pid]);

                    // из одной ТУ цепочку не построить
                    if (schema_points.length >= 1) {
                        if (ptype === 'http://trinidata.ru/sigma/ТочкаУчетаОсновная') {
                            schemas[schema_id].parents.push(pid);
                        }
                        else {
                            const pdir = p['НаправлениеУчетаРасчетнойСхемыТу'];
                            const type_id = ptype;
                            schemas[schema_id].childs.push({ id: pid, type: type_id, dir: pdir, calc_method: method });
                        }
                    }
                }
            }
        }

        // чистка свойств СР без ТУ 
        for (const key of Object.keys(schemas)) {
            const parents = schemas[key].parents;
            const childs = schemas[key].childs;
            if (parents.length === 0 || childs.length === 0) {
                delete schemas[key];
                continue;
            }
            for (const id of parents) {
                for (const cp of childs) {
                    chains.push({ parent_id: id, child_id: cp.id, type: cp.type, dir: cp.dir, calc_method: cp.calc_method });
                }
            }
        }

        this.transit = chains;
    }

    getCntPoints(){
        const points = {};
        for(const sup_point of this.nodes){
            for(const cnt_point of sup_point.nodes){
                points[cnt_point.pnt_kod_point] = cnt_point;
            }
        }
        return points;
    }

    setPointsProps() {
        for (const ap of this.nodes) {
            for (const cp of ap.nodes) {
                if (this.calc_schema.points[cp.pnt_kod_point]) {
                    cp.pnt_rs_props.calc_method = this.calc_schema.points[cp.pnt_kod_point].calc_method;
                }
            }
        }
    }

    /**
     * Получить все наименования столбцов: свои столбцы + столбцы инкапсулированных классов
     */
    static getColNames() {
        if (MdmDoc.col_names === null) {
            MdmDoc.col_names = [
                ...MdmDoc.getSelfColNames(),
                ...Addr.getColNames('abon_adrr'),
                ...Addr.getColNames('abon_adrf'),
                ...Addr.getColNames('nobj_adr'),
                ...MdmSupPoint.getColNames()
            ]
        }
        return MdmDoc.col_names;
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

                    // /// проверка на значение-массив (да-да и такое встречается!)
                    // let err = '';
                    // for (const i in row) {
                    //     const x = row[i];
                    //     if (x !== null) {
                    //         if (typeof x === 'object') {
                    //             BaseMsg.warn('BAD VALUE: ' + JSON.stringify(x));
                    //             if (Array.isArray(x)) {
                    //                 err += `column #${MdmDoc.col_names[i]} is array: ` + JSON.stringify(x) + ' ';
                    //             } else {
                    //                 err += `column #${MdmDoc.col_names[i]} is object` + JSON.stringify(x) + ' ';
                    //             }
                    //             row[i] = null;
                    //         }
                    //     }
                    // }
                    // if (err) {
                    //     BaseMsg.warn('HAS ERRORS: ' + err);
                    // }
                    rows.push(row);
                }
            }
        }
        this.validateValues(rows);
        return rows;
    }

    validateValues(rows) {
        const col_count = MdmDoc.getColNames().length;
        for (const row of rows) {
            try {
                for (let i = 0; i < col_count; i++) {
                    const x = row[i];
                    if (x !== null) {
                        if (typeof x === 'object') {
                            const error =
                                'BAD VALUE: ' +
                                MdmDoc.col_names[i] +
                                ' is ' + (Array.isArray(x) ? 'array' : 'object') +
                                JSON.stringify(x);
                            BaseMsg.warn(error);
                            this.errors.push(error);
                            row[i] = null;
                        }
                    }
                }
            }
            catch (ex) {
                this.errors.push(ex.message);
            }
        }
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