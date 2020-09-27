'use strict';

const EventEmitter = require('events');
const path = require('path');
const log = require('log4js').getLogger('worker');

const CONST = require('./resources/const.json');
// const CONST = require('./resources/const.json');
const DBHelper = require('./helpers/db_helper');
const FileHelper = require('./helpers/file_helper');

const hub = require('./framework/event_hub');

const Statistics = require('./consumers/statistics');
const MessageHandler = require('./consumers/message_handler');
const TestConsumer = require('./consumers/test_consumer');

const HttpClient = require('./producers/http_client');
const FileClient = require('./producers/file_client');
const FakeClient = require('./producers/fake_client');
// const MessageRecorder = require('./consumers/message_recorder');

module.exports = class Worker extends EventEmitter {

    constructor(cfg) {
        super();

        this.cfg = cfg;
        this.cfg.dbLimit = 10;
        this.cfg.fileLimit = 10;

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
}