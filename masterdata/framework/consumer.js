'use strict';

const EventEmitter = require('events');
const log = require('log4js').getLogger('consumer');
const hub = require('./event_hub');
const factory = require('./msg_factory');

module.exports = class Consumer extends EventEmitter {

    constructor(cfg) {
        super();
        
        this.runLimit = cfg.parallel;
        this.enabled = cfg.enabled;
        this.runCount = 1;

    }

    init(){
        if(this.enabled){
            hub.subscribe(this);
        }
    }

    /// возващает promise
    async onData(pack) {
        let id = null;
        try {
            id = pack.id;
            this.onStart();

            const msg = factory.build(pack);

            await this.processMsg(msg)
                .finally(() => {
                    this.onStop();
                });
        }
        catch (ex) {
            log.error(`${id}\t${ex.message}`);
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

}