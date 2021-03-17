'use strict';

const path = require('path');
const Adapter = require('../helpers/adapter');
const cfg = require('../../config');
const CONST = require('../resources/const.json');
const Consumer = require('../framework/consumer');
const { CalcScheme } = require('../models/md_common/calc_scheme');
const FileHelper = require('../helpers/file_helper');

module.exports = class TestConsumer extends Consumer {

    constructor(cfg) {
        super(cfg);

        this.delay = cfg.delay;
    }

    init(){
        const context = this;
        return new Promise((resolve, reject) => {
            super.init();
            resolve();
        });
    }

    async processMsg(msg) {
        if(msg === null) return;

        
        const data = JSON.parse(msg.data);
        Adapter.normalize(data, '', CONST.ARRAY_ROUTES.map(item => item.toLowerCase()));
        const nodes = Adapter.getNodes(data, 'ОбеспечиваетсяЭэЧерезТочкиПоставки/ИспользуетсяРасчетнаяСхема');
        console.log(`get ${nodes.length} nodes`);
        const schemes = CalcScheme.parse(nodes)
        console.log(`set ${schemes.length} schemes`);
        await FileHelper.saveObj(path.join(cfg.work_dir, 'dbg', 'scheme-'+ msg.id), schemes);
    }
}