'use strict';

const moment = require('moment');


module.exports = class Utils {

    static sleep(ms) {
        return new Promise((resolve) => {
            setTimeout(resolve, ms);
        });
    }

    static extractLastSegment(url) {
        return url ? url.substring(url.lastIndexOf('/') + 1) : '';
    }

    static getHash(str) {
        // return Array.from(str).reduce((s, c) => Math.imul(31, s) + c.charCodeAt(0) | 0, 0);
        return Math.abs( Array.from(str).reduce((s, c) => {
            return Math.imul(31, s) + c.charCodeAt(0) | 0
        }, 0));
    }

    static randomString(size) {
        var result = '';
        var characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
        var charactersLength = characters.length;
        for (var i = 0; i < size; i++) {
            result += characters.charAt(Math.floor(Math.random() * charactersLength));
        }
        return result;
    }

    static getTimeLabel(fmt){
        return moment().format(fmt ? fmt : 'YYYYMMDD-HHmmss-SSS');
    }
}

