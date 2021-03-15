'use strict';

module.exports = class Note{

    static get id() { return this.id; }
    static set id(val) { this.id = Note.normalizeId(val); }
    
    constructor(noteId, noteMessage){
        this.id = Note.normalizeId(noteId);
        this.message = noteMessage;
    }

    static normalizeId(id){
        return id.replace('http://trinidata.ru/sigma/', '');
    }

    joinId(parentId){
        this.id = Note.normalizeId(parentId) + '/' + this.id;
    }

}