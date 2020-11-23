'use strict';

const EventEmitter = require('events');
const log4js = require('log4js');
const hub = require('./event_hub');

module.exports = class Consumer extends EventEmitter {

    constructor(cfg) {
        super();

        this.runLimit = cfg.parallel;
        this.enabled = cfg.enabled;
        this.runCount = 1;

        this.log = log4js.getLogger(cfg.tag ? cfg.tag : 'consumer');
    }

    /// возващает promise
    async onData(msg) {
        try {
            this.onStart();

            await this.processMsg(msg)
                .finally(() => {
                    this.onStop();
                });
        }
        catch (ex) {
            const id = msg && msg.id ? msg.id : '-';
            this.log.error(`${id}\t${ex.message}`);
        }
    }

    onStart() {
        this.runCount++;
        if (!this.isReady()) {
            hub.setBusy()
        }
    }

    onStop() {
        this.runCount--;
        if (this.isReady()) {
            hub.setReady();
        }
    }

    isReady() {
        return this.runCount < this.runLimit;
    }

    info(msg){
        this.log.info(msg);
    }

    warn(msg){
        this.log.warn(msg);
    }

    error(msg){
        this.log.error(msg);
    }

    debug(msg){
        this.log.debug(msg);
    }

    init() {
        this.info(this.enabled ? 'READY' : 'SLEEP');
        if (this.enabled) {
            hub.subscribe(this);
        }
    }

}