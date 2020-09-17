const path = require('path');
const log4js = require('log4js');
const cfg_dev = require('./cfg.json');

const cfg = process.env.NODE_ENV === 'production' ? cfg_dev : cfg_dev;


if(process.env.NODE_ENV === 'production'){
    cfg.options = cfg.production;
} else {
    cfg.options = cfg.development;
}
cfg.db = cfg.database[cfg.options.db];


const home_dir = require('os').homedir();
const root_dir = path.join(home_dir, '../../');
const otp_dir = path.join(root_dir, '/otp');
const otp_app_dir = path.join(otp_dir, '/lenenergo.psk.integration');
const in_dir = path.join(otp_app_dir, '/IN');
cfg.msg_dir = path.join(in_dir, '/msg_dir');
cfg.err_dir = path.join(in_dir, '/error_dir');
cfg.log_dir = path.join(in_dir, '/logs_dir');
cfg.dbg_dir = path.join(in_dir, '/dbg_dir');


log4js.configure({
    appenders: {
        app: { type: 'dateFile', filename: path.join(cfg.log_dir, 'md.log'), pattern: '.yyyy-MM-dd', daysToKeep: 7 },
        console: { type: 'console' }
    },
    categories: {
        default: { appenders: ['app', 'console'], level: 'debug' }
    }
});

module.exports = cfg;