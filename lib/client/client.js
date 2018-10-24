'use strict';

const debug = require('debug')('rpc-client');
const Base = require('sdk-base');
const assert = require('assert');
const Scheduler = require('./scheduler');
const RpcConsumer = require('./consumer');
const protocol = require('sofa-bolt-node');
const RpcConnection = require('./connection/rpc');
const ConnectionManager = require('./connection_mgr');

const defaultOptions = {
  group: 'SOFA',
  version: '1.0',
  responseTimeout: 3000,
  consumerClass: RpcConsumer,
  connectionClass: RpcConnection,
  connectionManagerClass: ConnectionManager,
  connectionOpts: {
    protocol,
    noDelay: true, // 默认禁用 Nagle 算法
    connectTimeout: 3000, // 连接超时时长
  },
};

/**
 * -  _consumerCache: 保证 client 重复调用相同方法时，可以复用一个 consumer
 * -  rpcConsumer: 以 methodName 为维度，一个 client 调用不同方法时会创建多个 consumer，consumer 从连接池（connectionManager）中通过 address 复用连接
 * -  rpcConnection: 以 address 为维度，负责 invoke 链路: encode => socket => decode 的控制, 包含超时、熔断的机制。由 connectionManager 创建
 * -  connectionManager: 负责管理当前 client 所有的连接，之后会被传入consumer，以供取用。可以实现为单例
 * @class RpcClient
 * @extends {Base}
 */
class RpcClient extends Base {
  constructor(options = {}) {
    assert(options.logger, '[RpcClient] options.logger is required');
    options = Object.assign({}, defaultOptions, options);
    super(options);

    if (options.protocol) this.options.connectionOpts.protocol = options.protocol;

    // 确保一个服务只创建一个 consumer。
    this._consumerCache = new Map();
    this.connectionManager = new options.connectionManagerClass(options);
    this.connectionManager.on('error', err => { this.emit('error', err); });
    // 立马 ready
    this.ready(true);
  }

  close() {
    Scheduler.instance.clear();
    for (const consumer of this._consumerCache.values()) {
      consumer.close();
    }
    this._consumerCache.clear();
    return this.connectionManager.close();
  }

  get consumerMap() {
    return this._consumerCache;
  }

  get consumerClass() {
    return this.options.consumerClass;
  }

  set consumerClass(val) {
    this.options.consumerClass = val;
  }

  // 通常只是在单元测试时使用
  async invoke(opt) {
    const consumer = this.createConsumer(opt);
    const { methodName, args, options } = opt;
    await consumer.ready();
    return await consumer.invoke(methodName, args, options);
  }

  createConsumer(options, consumerClass) {
    assert(typeof options.interfaceName === 'string', '[RpcClient] createConsumer(options) options.interfaceName is required and should be a string.');
    options = Object.assign({
      connectionManager: this.connectionManager,
    }, this.options, options);
    const key = this.formatKey(options);
    let consumer = this._consumerCache.get(key);
    if (!consumer) {
      debug('create consumer for %s', key);
      consumerClass = consumerClass || this.consumerClass;
      consumer = new consumerClass(options);
      // delegate consumer's error to client
      consumer.on('error', err => { this.emit('error', err); });
      consumer.on('request', req => { this.emit('request', req); });
      consumer.on('response', info => { this.emit('response', info); });
      this._consumerCache.set(key, consumer);
    }
    return consumer;
  }

  formatKey(options) {
    const { interfaceName, version, group, serverHost } = options;
    let key = interfaceName + ':' + version + '@' + group;
    if (serverHost) {
      key += '@' + serverHost;
    }
    if (options.targetAppName) {
      key += '@' + options.targetAppName;
    }
    return key;
  }
}

module.exports = RpcClient;
