'use strict';

const EventEmitter = require('events');
const path = require('path');
const log = require('log4js').getLogger('worker');

const CONST = require('./resources/const.json');
// const CONST = require('./resources/const.json');
const DBHelper = require('./helpers/db_helper');
const FileHelper = require('./helpers/file_helper');

const hub = require('./framework/event_hub');
const factory = require('./framework/msg_factory');

const Statistics = require('./consumers/statistics');
const MessageHandler = require('./consumers/message_handler');
const TestConsumer = require('./consumers/test_consumer');

const HttpClient = require('./producers/http_client');
const FileClient = require('./producers/file_client');
const FakeClient = require('./producers/fake_client');
// const MessageRecorder = require('./consumers/message_recorder');

const BaseMsg = require('./framework/base_msg');
const SourceDoc = require('./models/mdm_src/source_doc');
const IndicatDoc = require('./models/num/indicat');
const VolumeDoc = require('./models/volumes/vol_doc');


module.exports = class Worker extends EventEmitter {

    constructor(cfg) {
        super();

        this.cfg = cfg;
        this.cfg.dbLimit = 10;
        this.cfg.fileLimit = 10;

        /// CONTRACT this.buildMessage 
        factory.setBuildProc(this.buildMessage);

        this.stat = new Statistics(cfg.consumers.statistics);
        this.msg_handler = new MessageHandler(cfg.consumers.message_handler);
        this.test_consumer = new TestConsumer(cfg.consumers.test_consumer);
        // this.msg_recorder = new MessageRecorder(cfg.consumers.message_recorder);

        this.http_cli = new HttpClient(cfg.producers.http_client);
        this.file_cli = new FileClient(cfg.producers.file_client);
        this.fake_cli = new FakeClient(cfg.producers.fake_client);
    }

    async check() {
        return true;
    }

    async start() {
        log.info('WORKER STARTING');

        await this.stat.init();
        await this.msg_handler.init();
        await this.test_consumer.init();

        await this.file_cli.start();
        await this.http_cli.start();
        await this.fake_cli.start();

        log.info('WORKER STARTED');
    }

    buildMessage(pack) {
        if (pack === null) {
            return null;
        }
        /// IDLE
        else if (pack.id === null && pack.data === null) {
            return null;
        }
        else {
            if (pack.code === 200) {
                try {
                    let ies_type = CONST.MSG_TYPES.NO_TYPE;
                    let jobj = null;
                    try{
                        jobj = JSON.parse(pack.data);
                        ies_type = jobj['@type'] ? jobj['@type'] : CONST.NO_TYPE;
                        jobj.id = pack.id;
                    }
                    catch(ex){
                        pack.data = ex.message;
                    }

                    if (ies_type === CONST.MSG_TYPES.TYPE_MDM) {
                        log.info(`${pack.id} [6.1]`);
                        return new SourceDoc(jobj);
                    } else if (ies_type === CONST.MSG_TYPES.TYPE_IND) {
                        log.info(`${pack.id} [13.1]`);
                        return new IndicatDoc(jobj);
                    } else if (ies_type === CONST.MSG_TYPES.TYPE_VOL) {
                        log.info(`${pack.id} [16.1]`);
                        return new VolumeDoc(jobj);
                    } else {
                        const msg = Worker.makeErrorMsg(pack, 'UNKNOWN-TYPE');
                        log.error(`${msg.id}\t${msg.error}`);
                        return msg;
                    }
                }
                catch(ex){
                    log.error(`${pack.id}\t${ex.message}\t${ies_type}\tindex.buildMessage()`);
                    throw ex;
                }
            }
            else {
                const msg = Worker.makeErrorMsg(pack, 'UNCORRECTED');
                log.error(`${msg.id}\t${msg.code}\t${msg.error}`);
                return msg;
            }
        }
    }

    static makeErrorMsg(pack, error) {
        const msg = new BaseMsg(pack);
        msg.id = pack.id;
        msg.raw = pack.data;
        msg.error = error;
        return msg;
    }
}