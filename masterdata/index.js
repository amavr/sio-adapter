'use strict';

const EventEmitter = require('events');
const log = require('log4js').getLogger('WORKER');

const cfg = require('./../config');

const db_helper = require('./helpers/db_helper');
const db_refs = require('./helpers/db_refs');

const factory = require('./framework/msg_factory');

const Statistics = require('./consumers/statistics');
const MessageHandler = require('./consumers/message_handler');
const TestConsumer = require('./consumers/test_consumer');

const HttpClient = require('./producers/http_client');
const FileClient = require('./producers/file_client');
const FakeClient = require('./producers/fake_client');

const X = require('./test');

module.exports = class Worker extends EventEmitter {

    constructor() {
        super();

        this.cfg = cfg;
        this.cfg.dbLimit = 10;
        this.cfg.fileLimit = 10;

        /// CONTRACT this.buildMessage 
        factory.setBuildProc(this.buildMessage);

        const stat = new Statistics(cfg.consumers.statistics);
        this.msg_handler = new MessageHandler(cfg.consumers.message_handler, stat);
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
        log.info('================================');
        log.info('INITIALIZE RESOURCES ...');

        await db_helper.init(cfg.databases[cfg.dbname].hrPool);
        await db_refs.init(db_helper);

        log.info('COMPLETE');
        log.info('================================');

        // X.pointsAndSchemasAll();
        // X.processFiles('D:/IE/files/2020-09/out_message6_1_1410');

        // log.info('START BROKEN');
        // log.info('================================');
        // return;

        log.info('================================');
        log.info('INITIALIZE HANDLERS ...');

        await this.msg_handler.init();
        await this.test_consumer.init();

        await this.file_cli.start();
        await this.http_cli.start();
        await this.fake_cli.start();

        log.info('COMPLETE');
        log.info('================================');
    }

}