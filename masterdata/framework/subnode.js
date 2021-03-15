'use strict';

module.exports = class SubNode{

    constructor(){
        this.id = null;
        this.tag = 'SubNode';
        this.warnings = [];
        this.errors = [];
    }

    addWarning(msg){
        this.warnings.push(msg);
    }

    addError(msg){
        this.errors.push(msg);
    }

    getWarnings(){
        
    }

    getErrors(){
        
    }
}