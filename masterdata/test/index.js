'use strict';

const path = require('path');
const FileHelper = require('../helpers/file_helper');
const Adapter = require('../helpers/adapter');
const db_helper = require('../helpers/db_helper');
const db_refs = require('../helpers/db_refs');
const { type } = require('os');

const SOUR_DIR = 'D:/IE/files/2020-09/out_message6_1_1410';
const BASE_DIR = 'D:/IE/files/test/transit';

const MASTER = 'ul-0000190.json';
const TRANSITS = ['ul-0037493.json', 'ul-0089792.json', 'ul-0125651.json', 'ul-0061426.json', 'ul-0082280.json'];

const SQL = 'insert into sio_point_chains(master_ies, detail_ies, kod_directen, detail_type, calc_method) values(:1, :2, :3, :4, :5)';

module.exports = class X {
    constructor() {

    }

    static async processFiles(dirPath){
        const files = await FileHelper.getFiles(dirPath);
        console.log(`files: ${files.length}`);
        let i = 0;
        for(const f of files){
            await this.saveChains(path.join(dirPath, f));
            i++;
            if(i % 1000 === 0){
                console.log(`${i}`);
            }
        }
    }

    static async pointsAndSchemasAll() {
        await this.saveChains(path.join(BASE_DIR, MASTER));
        for (const fname of TRANSITS) {
            await this.saveChains(path.join(BASE_DIR, fname));
        }
    }

    static async saveChains(filePath) {
        let doc = await FileHelper.readAsObject(filePath);
        await this.normalizeDoc(doc);
        let chains = await this.extractTransitChains(doc);
        if (chains.length > 0) {
            this.saveTransitChains(chains);
            // FileHelper.saveObj(FileHelper.changeFileExt(filePath, '.2db.json'), chains);

        }
    }

    static async saveTransitChains(chains){
        const rows = chains.map(r => [r.parent_id, r.child_id, r.dir, r.type, r.calc_method]);
        const res = await db_helper.insertMany(SQL, rows);
        // console.log(res);
    }

    static async extractTransitChains(doc) {
        const chains = [];
        const schemas = {};

        // Обход точек подключения
        const attp_points = Adapter.getVal(doc, 'ОбеспечиваетсяЭэЧерезТочкиПоставки');
        if(attp_points === null) return chains;

        for (const ap of attp_points) {

            // перебор всех СР и транзитных цепочек
            const used_schemas = Adapter.getVal(ap, 'ИспользуетсяРасчетнаяСхема');
            if(used_schemas === null) continue;

            for (const sch of used_schemas) {
                const schema_id = sch['@id'];
                schemas[schema_id] = { parents: [], childs: [] }
                const schema_points = Adapter.getVal(sch, 'ТочкаУчетаВРасчетнойСхеме');
                if (schema_points === null) continue;
                // из одной ТУ цепочку не построить
                if (schema_points.length < 2) continue;

                // перебор точек СР
                for (const p of schema_points) {
                    const ptype = p['ИмеетТипТочкиУчета'];

                    const cnt_point = Adapter.getVal(p, 'ЯвляетсяТУ');
                    if (cnt_point === null) continue;

                    const pid = typeof cnt_point === 'object' ? cnt_point['@id'] : cnt_point;
                    if (ptype === 'http://trinidata.ru/sigma/ТочкаУчетаОсновная') {
                        schemas[schema_id].parents.push(pid);
                    }
                    else {
                        const pdir = p['НаправлениеУчетаРасчетнойСхемыТу'];
                        const type_id = ptype;
                        const method = p['ИмеетМетодРасчета'];
                        // const pdir = this.translateEnergyDirection(p['НаправлениеУчетаРасчетнойСхемыТу']);
                        // const type_id = this.translatePointType(ptype);
                        schemas[schema_id].childs.push({ id: pid, type: type_id, dir: pdir, calc_method: method });
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

        return chains;
    }

    static translatePointType(pointType) {
        return pointType.includes('ТочкаУчетаТранзитная') ? 'TRANS'
            : pointType.includes('ТуДочерняя') ? 'CHILD'
                : pointType.includes('ТочкаУчетаОсновная') ? 'BASE'
                    : null;
    }

    static translateEnergyDirection(direction) {
        return db_refs.link_dicts.Direction[direction];
    }

    static async normalizeAll() {
        this.normalizeFile(path.join(SOUR_DIR, MASTER));
        for (const fname of TRANSITS) {
            this.normalizeFile(path.join(SOUR_DIR, fname));
        }
    }

    static async normalizeDoc(doc) {
        Adapter.normalize(doc, '',
            [
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки',
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки/ИспользуетсяРасчетнаяСхема',
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки/ИспользуетсяРасчетнаяСхема/ТочкаУчетаВРасчетнойСхеме',
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки/ТочкаУчетаРасчетная',
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки/ТочкаУчетаРасчетная/ИзмерительныйКомплексНаТу',
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки/ТочкаУчетаРасчетная/ИзмерительныйКомплексНаТу/ПуНаИк',
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки/ТочкаУчетаРасчетная/ИзмерительныйКомплексНаТу/ПуНаИк/РегистрНаПу'
            ].map(item => item.toLowerCase()));
    }

    static async normalizeFile(doc) {
        // const doc = await FileHelper.readAsObject(filePath);
        Adapter.normalize(doc, '',
            [
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки',
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки/ИспользуетсяРасчетнаяСхема',
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки/ИспользуетсяРасчетнаяСхема/ТочкаУчетаВРасчетнойСхеме',
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки/ТочкаУчетаРасчетная',
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки/ТочкаУчетаРасчетная/ИзмерительныйКомплексНаТу',
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки/ТочкаУчетаРасчетная/ИзмерительныйКомплексНаТу/ПуНаИк',
                '/ОбеспечиваетсяЭэЧерезТочкиПоставки/ТочкаУчетаРасчетная/ИзмерительныйКомплексНаТу/ПуНаИк/РегистрНаПу'
            ].map(item => item.toLowerCase()));

        // const fname = FileHelper.changeFileDir(filePath, BASE_DIR);
        // FileHelper.saveObj(fname, doc);
    }

    static async pointsAndSchemas(filePath) {
        const doc = await FileHelper.readAsObject(filePath);

        const points = {};
        const schemas = {};

        // Обход точек подключения
        const attp_points = Adapter.getVal(doc, 'ОбеспечиваетсяЭэЧерезТочкиПоставки');
        for (const ap of attp_points) {

            // сбор точек учета
            const cnt_points = Adapter.getVal(ap, 'ТочкаУчетаРасчетная');
            for (const p of cnt_points) {
                points[p['@id']] = { isTransit: p['ЯвляетсяТранзитной'], schemas: [] };
            }

            // установка у каждой ТУ схемы расчета: перебор всех СР и их назначение входящим ТУ
            const used_schemas = Adapter.getVal(ap, 'ИспользуетсяРасчетнаяСхема');
            for (const sch of used_schemas) {
                const schema_id = sch['@id'];
                schemas[schema_id] = [];
                const schema_points = Adapter.getVal(sch, 'ТочкаУчетаВРасчетнойСхеме');
                if (schema_points === null) continue;

                // перебор точек СР
                for (const p of schema_points) {
                    const ptype = p['ИмеетТипТочкиУчета'];

                    const cnt_point = Adapter.getVal(p, 'ЯвляетсяТУ');
                    if (cnt_point === null) continue;

                    const pid = typeof cnt_point === 'object' ? cnt_point['@id'] : cnt_point;
                    schemas[schema_id].push({ id: pid, type: ptype });

                    // есть в списке
                    if (points[pid]) points[pid].schemas.push(schema_id);
                }
            }
        }

        // чистка свойств СР без ТУ 
        for (const key of Object.keys(schemas)) {
            if (schemas[key].length === 0) delete schemas[key];
        }
        // чистка свойств ТУ без СР
        for (const key of Object.keys(points)) {
            if (points[key].schemas.length === 0) delete points[key];
        }



        FileHelper.saveObj(filePath + '.txt', {
            P: points,
            S: schemas
        });
    }

}
