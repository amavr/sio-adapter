'use strict';

module.exports = class Result {

    constructor(){
        this.id = null;
        this.register_id = null;
        this.value = 0;
    }

    static parse(node) {
        const items = [];
        let nodes = [];
        if (Array.isArray(node)) {
            nodes = node;
        }
        else {
            nodes.push(node);
        }
        nodes.forEach(n => {
            const item = new Result();
            item.id = n['@id'];
            item.register_id = n['ОтноситсяКРегиструПу'];
            item.value = n['ЗначениеПоказаний'];
            items.push(item);
        });

        return items;
    }


}