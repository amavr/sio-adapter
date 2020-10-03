'use strict';

class MsgFactory{
    constructor(){
        this.build = null;
        console.log('MsgFactory created');
    }

    setBuildProc(messageBuildProc){
        this.build = messageBuildProc;
    }

    createMsg(pack){
        if(this.build){
            return this.build(pack);
        }
        else{
            return null;
        }
    }
}

module.exports = new MsgFactory();