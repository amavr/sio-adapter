'use strict';

const EventEmitter = require('events');
const path = require('path');
const log = require('log4js').getLogger('worker');

const CONST = require('./resources/const.json');
// const CONST = require('./resources/const.json');
const DBHelper = require('./helpers/db_helper');
const FileHelper = require('./helpers/file_helper');

const hub = require('./event_hub');
const stat = require('./statistics');
const HttpClient = require('./http_client');
const FileClient = require('./file_client');
const MessageHandler = require('./message_handler');
const MessageRecorder = require('./message_recorder');

module.exports = class Worker extends EventEmitter {

    constructor(cfg) {
        super();

        this.cfg = cfg;

        this.msg_handler = new MessageHandler(this.cfg);
        this.msg_recorder = new MessageRecorder(this.cfg);
        this.stat = stat;
        this.cli = new HttpClient(this.cfg);

        hub.subscribe(CONST.EVENTS.ON_NEW_MESSAGE, this.msg_recorder);
        hub.subscribe(CONST.EVENTS.ON_NEW_MESSAGE, this.msg_handler);

        hub.subscribe(CONST.EVENTS.ON_NEW_MESSAGE, this.stat);
        hub.subscribe(CONST.EVENTS.ON_RECEIVE_ERR, this.stat);
        hub.subscribe(CONST.EVENTS.ON_RECEIVER_IDLE, this.stat);
        hub.subscribe(CONST.EVENTS.ON_DB_CALL, this.stat);

        this.fcli = new FileClient({
                watch_dir: 'D:/IE/files/buf/watch',
                backup_dir: 'D:/IE/files/buf/results',
                interval: 2000
            }
        );

        // this.cli.addSubscriber(this.rec);
        // this.cli.addSubscriber(this.hdl);
    }

    async check(){
        return true;
    }

    async start() {
        log.info('WORKER STARTING');
        // this.cli.exec();
        await this.msg_handler.init();
        await this.fcli.start();
        log.info('WORKER STARTED');
    }
}