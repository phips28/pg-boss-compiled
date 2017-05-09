'use strict';

var _createClass = function () {
  function defineProperties(target, props) {
    for (var i = 0; i < props.length; i++) {
      var descriptor = props[i];
      descriptor.enumerable = descriptor.enumerable || false;
      descriptor.configurable = true;
      if ("value" in descriptor) descriptor.writable = true;
      Object.defineProperty(target, descriptor.key, descriptor);
    }
  }

  return function (Constructor, protoProps, staticProps) {
    if (protoProps) defineProperties(Constructor.prototype, protoProps);
    if (staticProps) defineProperties(Constructor, staticProps);
    return Constructor;
  };
}();

function _classCallCheck(instance, Constructor) {
  if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); }
}

function _possibleConstructorReturn(self, call) {
  if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); }
  return call && (typeof call === "object" || typeof call === "function") ? call : self;
}

function _inherits(subClass, superClass) {
  if (typeof superClass !== "function" && superClass !== null) {
    throw new TypeError("Super expression must either be null or a function, not " + typeof superClass);
  }
  subClass.prototype = Object.create(superClass && superClass.prototype,
    { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } });
  if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass;
}

var EventEmitter = require('events');
var assert = require('assert');
var Promise = require("bluebird");
var Attorney = require('./attorney');
var Contractor = require('./contractor');
var Manager = require('./manager');
var Boss = require('./boss');
var Db = require('./db');

var notReadyErrorMessage = 'boss ain\'t ready.  Use start() or connect() to get started.';

var PgBoss = function (_EventEmitter) {
  _inherits(PgBoss, _EventEmitter);

  _createClass(PgBoss, null, [{
    key: 'getConstructionPlans',
    value: function getConstructionPlans(schema) {
      return Contractor.constructionPlans(schema);
    }
  }, {
    key: 'getMigrationPlans',
    value: function getMigrationPlans(schema, version, uninstall) {
      return Contractor.migrationPlans(schema, version, uninstall);
    }
  }]);

  function PgBoss(config) {
    _classCallCheck(this, PgBoss);

    config = Attorney.applyConfig(config);

    var _this = _possibleConstructorReturn(this, (PgBoss.__proto__ || Object.getPrototypeOf(PgBoss)).call(this));

    _this.config = config;

    var db = new Db(config);

    promoteEvent.call(_this, db, 'error');

    // contractor makes sure we have a happy database home for work
    _this.contractor = new Contractor(db, config);

    // boss keeps the books and archives old jobs
    var boss = new Boss(db, config);
    _this.boss = boss;

    ['error', 'archived'].forEach(function (event) {
      return promoteEvent.call(_this, boss, event);
    });

    // manager makes sure workers aren't taking too long to finish their jobs
    var manager = new Manager(db, config);
    _this.manager = manager;

    ['error', 'job', 'expired-job', 'expired-count', 'failed'].forEach(function (event) {
      return promoteEvent.call(_this, manager, event);
    });

    ['fetch', 'complete', 'cancel', 'fail', 'publish', 'subscribe', 'unsubscribe', 'onExpire'].forEach(function (func) {
      return promoteApi.call(_this, manager, func);
    });

    function promoteApi(obj, func) {
      var _this2 = this;

      this[func] = function () {
        for (var _len = arguments.length, args = Array(_len), _key = 0; _key < _len; _key++) {
          args[_key] = arguments[_key];
        }

        if (!_this2.isReady) return Promise.reject(notReadyErrorMessage);
        return obj[func].apply(obj, args);
      };
    }

    function promoteEvent(emitter, event) {
      var _this3 = this;

      emitter.on(event, function (arg) {
        return _this3.emit(event, arg);
      });
    }

    return _this;
  }

  _createClass(PgBoss, [{
    key: 'init',
    value: function init() {
      var _this4 = this;

      if (this.isReady) return Promise.resolve(this);

      return this.boss.supervise().then(function () {
        return _this4.manager.monitor();
      }).then(function () {
        _this4.isReady = true;
        return _this4;
      });
    }
  }, {
    key: 'start',
    value: function start() {
      var self = this;

      if (this.isStarting) return Promise.reject(
        'boss is starting up. Please wait for the previous start() to finish.');

      this.isStarting = true;

      for (var _len2 = arguments.length, args = Array(_len2), _key2 = 0; _key2 < _len2; _key2++) {
        args[_key2] = arguments[_key2];
      }

      return this.contractor.start.apply(this.contractor, args).then(function () {
        self.isStarting = false;
        return self.init();
      });
    }
  }, {
    key: 'stop',
    value: function stop() {
      return Promise.all([this.disconnect(), this.manager.stop(), this.boss.stop()]);
    }
  }, {
    key: 'connect',
    value: function connect() {
      var self = this;

      for (var _len3 = arguments.length, args = Array(_len3), _key3 = 0; _key3 < _len3; _key3++) {
        args[_key3] = arguments[_key3];
      }

      return this.contractor.connect.apply(this.contractor, args).then(function () {
        self.isReady = true;
        return self;
      });
    }
  }, {
    key: 'disconnect',
    value: function disconnect() {
      var _this5 = this;

      if (!this.isReady) return Promise.reject(notReadyErrorMessage);

      for (var _len4 = arguments.length, args = Array(_len4), _key4 = 0; _key4 < _len4; _key4++) {
        args[_key4] = arguments[_key4];
      }

      return this.manager.close.apply(this.manager, args).then(function () {
        return _this5.isReady = false;
      });
    }
  }]);

  return PgBoss;
}(EventEmitter);

module.exports = PgBoss;