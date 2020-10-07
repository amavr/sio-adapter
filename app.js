'use strict';

const createError = require('http-errors');
const express = require('express');
const path = require('path');
const cookieParser = require('cookie-parser');
const oracledb = require('oracledb');

const cfg = require('./config');

const log4js = require('log4js');
const log = log4js.getLogger('app');
log.level = 'debug';


const ofs = 10;
log.info("".padEnd(32, '='));
log.info("NODE_ENV:".padStart(ofs) + process.env.NODE_ENV);
log.info("Platform:".padStart(ofs) + process.platform);
log.info("Version:".padStart(ofs) + process.version);
log.info("Arch:".padStart(ofs) + process.arch);
log.info("OracleDB:".padStart(ofs) + oracledb.versionString);
log.info("Client:".padStart(ofs) + oracledb.oracleClientVersionString);
log.info("".padEnd(32, '='));




const indexRouter = require('./routes');
const Worker = require('./masterdata');
const worker = new Worker(cfg);

const app = express();

// view engine setup
app.set('views', path.join(__dirname, 'views'));


// log.info('SERVER STARTING');

// app.use(log4js.connectLogger(log, { 
//     level: 'info' , 
//     format: (req, res, format) => { 
//         return format(`:status :remote-addr :method :url :content-length ${res.headers['msg-id']}`);
//     }
// }));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', indexRouter);

// catch 404 and forward to error handler
app.use(function (req, res, next) {
    next(createError(404));
});

// error handler
app.use(function (err, req, res, next) {
    // set locals, only providing error in development
    res.locals.message = err.message;
    res.locals.error = req.app.get('env') === 'development' ? err : {};
    // render the error page
    res.status(err.status || 500).json('Sorry, not found');
});

worker.start();

module.exports = app;
