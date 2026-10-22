/*
 * (c) Copyright Ascensio System SIA 2010-2024
 *
 * This program is a free software product. You can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License (AGPL)
 * version 3 as published by the Free Software Foundation. In accordance with
 * Section 7(a) of the GNU AGPL its Section 15 shall be amended to the effect
 * that Ascensio System SIA expressly excludes the warranty of non-infringement
 * of any third-party rights.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR  PURPOSE. For
 * details, see the GNU AGPL at: http://www.gnu.org/licenses/agpl-3.0.html
 *
 * The  interactive user interfaces in modified source and object code versions
 * of the Program must display Appropriate Legal Notices, as required under
 * Section 5 of the GNU AGPL version 3.
 *
 * All the Product's GUI elements, including illustrations and icon sets, as
 * well as technical writing content are licensed under the terms of the
 * Creative Commons Attribution-ShareAlike 4.0 International. See the License
 * terms at http://creativecommons.org/licenses/by-sa/4.0/legalcode
 *
 */

'use strict';
const config = require('config');
const amqp = require('amqplib');
const operationContext = require('./operationContext');

const cfgRabbitUrl = config.get('rabbitmq.url');
const cfgRabbitSocketOptions = config.util.cloneDeep(config.get('rabbitmq.socketOptions'));

const RECONNECT_TIMEOUT = 1000;

function connetPromise(closeCallback) {
  return new Promise((resolve, _reject) => {
    function startConnect() {
      amqp.connect(cfgRabbitUrl, cfgRabbitSocketOptions).then(
        conn => {
          conn.on('error', err => {
            operationContext.global.logger.error('[AMQP] conn error', err.stack);
          });
          const closeEventCallback = function () {
            //in some case receive multiple close events
            conn.removeListener('close', closeEventCallback);
            operationContext.global.logger.debug('[AMQP] conn close');
            closeCallback();
          };
          conn.on('close', closeEventCallback);
          operationContext.global.logger.debug('[AMQP] connected');
          resolve(conn);
        },
        err => {
          operationContext.global.logger.error('[AMQP] %s', err.stack);
          setTimeout(startConnect, RECONNECT_TIMEOUT);
        }
      );
    }
    startConnect();
  });
}
/**
 * Builds the handler a channel calls when it dies while its connection may stay open: a failed operation,
 * a broker basic.cancel (a cluster node went down) or an admin close. Without it the owner keeps a dead channel and never reconnects.
 * The handler closes the connection once, so the owner's connection 'close' callback runs the usual reconnect.
 * @param {Object} owner - taskqueue or pubsub with isClose, reconnecting and connection
 * @param {Object} conn - the connection the channels belong to
 * @returns {function(string): void}
 */
function createOnChannelDead(owner, conn) {
  return function (reason) {
    //a lost connection closes its channels before it emits 'close', so wait for that 'close' to clear the owner
    setImmediate(() => {
      if (owner.isClose || owner.reconnecting || owner.connection !== conn) {
        return;
      }
      owner.reconnecting = true;
      operationContext.global.logger.warn('[AMQP] channel %s, closing the connection to reconnect', reason);
      closePromise(conn).catch(err => {
        operationContext.global.logger.warn('[AMQP] close after channel %s: %s', reason, err.stack);
      });
    });
  };
}
function watchChannel(channel, opt_onChannelDead) {
  if (opt_onChannelDead) {
    channel.on('error', err => {
      operationContext.global.logger.error('[AMQP] channel error: %s', err.stack);
      opt_onChannelDead('error');
    });
    channel.on('close', () => {
      opt_onChannelDead('close');
    });
    channel.on('cancel', () => {
      opt_onChannelDead('consumer cancelled by the broker');
    });
  }
  return channel;
}
async function createChannelPromise(conn, opt_onChannelDead) {
  return watchChannel(await conn.createChannel(), opt_onChannelDead);
}
async function createConfirmChannelPromise(conn, opt_onChannelDead) {
  return watchChannel(await conn.createConfirmChannel(), opt_onChannelDead);
}
async function assertExchangePromise(channel, exchange, type, options) {
  const ok = await channel.assertExchange(exchange, type, options);
  return ok.exchange;
}

/**
 * Filters queue options based on queue type to prevent RabbitMQ PRECONDITION_FAILED errors.
 * Different queue types support different parameters:
 * - Classic queues: support all parameters including maxPriority, messageTtl, deadLetterExchange
 * - Quorum queues: do NOT support maxPriority (x-max-priority)
 * - Stream queues: do NOT support maxPriority, messageTtl, deadLetterExchange
 *
 * @param {Object} options - Queue options to filter
 * @param {string} queueName - Queue name for logging
 * @returns {Object} Filtered options compatible with queue type
 */
function filterQueueOptions(options, queueName) {
  if (!options) {
    return options;
  }

  const queueType = options.arguments && options.arguments['x-queue-type'];
  const unsupportedParams = {
    quorum: ['maxPriority'],
    stream: ['maxPriority', 'messageTtl', 'deadLetterExchange']
  };

  const paramsToRemove = unsupportedParams[queueType];
  if (!paramsToRemove) {
    return options;
  }

  const filteredOptions = {...options};
  const removedParams = [];

  for (const param of paramsToRemove) {
    if (filteredOptions[param] !== undefined) {
      delete filteredOptions[param];
      removedParams.push(param === 'maxPriority' ? 'maxPriority (x-max-priority)' : param);
    }
  }

  if (removedParams.length > 0) {
    operationContext.global.logger.info(
      'RabbitMQ %s queue "%s" does not require: %s. Parameters removed (RabbitMQ 4.0+ ignores these automatically, removed for compatibility with 3.x).',
      queueType,
      queueName,
      removedParams.join(', ')
    );
  }

  return filteredOptions;
}

async function assertQueuePromise(channel, queue, options) {
  const ok = await channel.assertQueue(queue, options);
  return ok.queue;
}
async function bindQueuePromise(channel, queue, source, pattern, opt_args) {
  return channel.bindQueue(queue, source, pattern, opt_args);
}
async function checkQueuePromise(channel, queue) {
  return channel.checkQueue(queue);
}
async function consumePromise(channel, queue, messageCallback, options) {
  return channel.consume(queue, messageCallback, options);
}
/**
 * Acks a message on the channel that delivered it.
 * A null message (a broker basic.cancel) is skipped. A closed channel throws; its delivery tags are void and the broker redelivers.
 * @param {Object} channel
 * @param {Object} message
 * @param {string} label - consumer name for logging
 * @returns {boolean} true if the ack was sent
 */
function safeAck(channel, message, label) {
  if (!channel || !message) {
    return false;
  }
  try {
    channel.ack(message);
    return true;
  } catch (err) {
    operationContext.global.logger.warn('[AMQP] %s ack skipped: %s', label, err.message);
    return false;
  }
}
/**
 * Resolves true if the check resolves to a truthy value within timeoutMs, otherwise false; never rejects.
 * @param {Promise} checkPromise
 * @param {number} timeoutMs
 * @param {string} label - check name for logging
 * @returns {Promise<boolean>}
 */
function runHealthCheck(checkPromise, timeoutMs, label) {
  let timer;
  const timeout = new Promise(resolve => {
    timer = setTimeout(() => {
      operationContext.global.logger.warn('[AMQP] %s timed out after %d ms', label, timeoutMs);
      resolve(false);
    }, timeoutMs);
  });
  const check = Promise.resolve(checkPromise).then(
    res => !!res,
    err => {
      operationContext.global.logger.warn('[AMQP] %s failed: %s', label, err?.message);
      return false;
    }
  );
  return Promise.race([check, timeout]).finally(() => clearTimeout(timer));
}
//closes a connection or a channel
async function closePromise(conn) {
  await conn.close();
}

module.exports.connetPromise = connetPromise;
module.exports.createChannelPromise = createChannelPromise;
module.exports.createConfirmChannelPromise = createConfirmChannelPromise;
module.exports.assertExchangePromise = assertExchangePromise;
module.exports.filterQueueOptions = filterQueueOptions;
module.exports.assertQueuePromise = assertQueuePromise;
module.exports.bindQueuePromise = bindQueuePromise;
module.exports.checkQueuePromise = checkQueuePromise;
module.exports.consumePromise = consumePromise;
module.exports.closePromise = closePromise;
module.exports.createOnChannelDead = createOnChannelDead;
module.exports.safeAck = safeAck;
module.exports.runHealthCheck = runHealthCheck;
module.exports.RECONNECT_TIMEOUT = RECONNECT_TIMEOUT;
