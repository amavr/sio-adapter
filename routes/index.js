'use strict';

const express = require('express');
const api = require('./api');

const router = express.Router();
router.use('/api', api);

router.get('/', function (req, res, next) {
  res.json({ msg: 'I`m alive' });
});

module.exports = router;
