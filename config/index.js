const path = require('path');
const log4js = require('log4js');

const cfg = process.env.NODE_ENV === 'production' ? require('./production.json') : require('./development.json');

if (process.env.NODE_ENV === 'production') {
    const home_dir = require('os').homedir();
    const root_dir = path.join(home_dir, '../../');
    const otp_dir = path.join(root_dir, '/opt');
    const otp_app_dir = path.join(otp_dir, '/lenenergo.psk.integration');
    cfg.work_dir = path.join(otp_app_dir, '/IN');
} else {
    cfg.work_dir = 'D:/IE/otp/lenenergo.psk.integration/IN';
}

if(cfg.api.dbname){
    cfg.api.db = cfg.databases[cfg.api.dbname].hrPool;
}

for(const key of Object.keys(cfg.consumers)){
    cfg.consumers[key].work_dir = cfg.work_dir;
    if(cfg.consumers[key].dbname){
        cfg.consumers[key].db = cfg.databases[cfg.consumers[key].dbname].hrPool;
    }
}

for(const key of Object.keys(cfg.producers)){
    cfg.producers[key].work_dir = cfg.work_dir;
    if(cfg.producers[key].dbname){
        cfg.producers[key].db = cfg.databases[cfg.producers[key].dbname].hrPool;
    }
}

log4js.configure({
    appenders: {
        app: { type: 'dateFile', filename: path.join(cfg.work_dir, cfg.log_dir, 'md.log'), pattern: '.yyyy-MM-dd', daysToKeep: 7 },
        console: { type: 'console' }
    },
    categories: {
        default: { appenders: ['app', 'console'], level: cfg.log_level }
    }
});

module.exports = cfg;