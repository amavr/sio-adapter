'use strict';

const fh = require('./file_helper');

class Stat {

    constructor(filePath) {

        fh.read(filePath)
            .then(json => {
                this.stat = JSON.parse(json);
                this.stat.last = { beg: new Date() };
            })
            .catch(err => {
                this.stat = {
                    uptimes: [],
                    uploads: []
                };
                this.stat.last = { beg: new Date() };
            });


        process.stdin.resume();//so the program will not close instantly

        //do something when app is closing
        process.on('exit', this.exitHandler.bind(null, { cleanup: true, cause: 'exit' }));

        //catches ctrl+c event
        process.on('SIGINT', this.exitHandler.bind(null, { exit: true, cause: 'ctrl+c' }));

        // catches "kill pid" (for example: nodemon restart)
        process.on('SIGUSR1', this.exitHandler.bind(null, { exit: true, cause: 'kill pid' }));
        process.on('SIGUSR2', this.exitHandler.bind(null, { exit: true, cause: 'kill pid' }));

        //catches uncaught exceptions
        process.on('uncaughtException', this.exitHandler.bind(null, { exit: true, cause: 'exception' }));
    }

    exitHandler(options, exitCode) {
        // console.log(options.stat);
        // options.stat.last.end = { time: new Date(), code: exitCode, cause: options.cause };
        // options.stat.uptimes.push(this.current_event);
        // delete options.stat.last;

        if (options.cleanup) console.log('clean');
        if (exitCode || exitCode === 0) console.log(exitCode);
        if (options.exit) process.exit();

        // fh.save(cfg.stat.file, options.stat)
        //     .then(x => {

        //         if (options.exit) process.exit();
        //     })
        //     .catch(err => {

        //         if (options.exit) process.exit();
        //     });
    }

    get statistic() {
        return this.stat;
    }

    set statistic(val) {
        this.stat = val;
    }

    get count() {
        return this.stat.uptimes.length;
    }


}

class Singleton {

    constructor() {
        if (!Singleton.instance) {
            Singleton.instance = new Stat();
        }
    }

    getInstance() {
        return Singleton.instance;
    }
}

module.exports = Singleton;