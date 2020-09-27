'use strict';

const EventEmitter = require('events');
const log = require('log4js').getLogger('consumer');
const hub = require('./event_hub');

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
        try {
            this.onStart();
            /// не следует перехватывать ошибки в executeEvent
            await this.executeEvent(pack)
                .finally(() => { 
                    this.onStop(); 
                });
        }
        catch (ex) {
            log.error(ex.message);
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