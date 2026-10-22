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
 */

const {describe, test, expect, afterEach, jest} = require('@jest/globals');
const EventEmitter = require('events');

const amqp = require('../../Common/node_modules/amqplib');
const operationContext = require('../../Common/sources/operationContext');
const rabbitMQCore = require('../../Common/sources/rabbitMQCore');

const nextTurn = () => new Promise(resolve => setImmediate(resolve));

function fakeConnection() {
  const conn = new EventEmitter();
  conn.close = jest.fn().mockResolvedValue();
  conn.createChannel = jest.fn(async () => new EventEmitter());
  conn.createConfirmChannel = jest.fn(async () => new EventEmitter());
  return conn;
}

// What taskqueueRabbitMQ and pubsubRabbitMQ hold while connected.
function fakeOwner(conn) {
  return {isClose: false, reconnecting: false, connection: conn};
}

afterEach(() => {
  jest.useRealTimers();
  jest.restoreAllMocks();
});

describe('rabbitMQCore createOnChannelDead', () => {
  test('a channel that dies on an open connection closes the connection once, so the owner reconnects', async () => {
    const conn = fakeConnection();
    const owner = fakeOwner(conn);
    const onChannelDead = rabbitMQCore.createOnChannelDead(owner, conn);

    onChannelDead('error');
    onChannelDead('close');
    await nextTurn();
    onChannelDead('close');
    await nextTurn();

    expect(conn.close).toHaveBeenCalledTimes(1);
    expect(owner.reconnecting).toBe(true);
  });

  test('a lost connection that clears the owner before the next turn is not closed again', async () => {
    const conn = fakeConnection();
    const owner = fakeOwner(conn);
    const onChannelDead = rabbitMQCore.createOnChannelDead(owner, conn);

    // amqplib closes the channels, then the connection emits 'close' and the owner's clear() runs in the same turn
    onChannelDead('close');
    owner.connection = null;
    await nextTurn();

    expect(conn.close).not.toHaveBeenCalled();
  });

  test('a channel of an earlier connection does not close the current one', async () => {
    const oldConn = fakeConnection();
    const owner = fakeOwner(fakeConnection());
    const onChannelDead = rabbitMQCore.createOnChannelDead(owner, oldConn);

    onChannelDead('close');
    await nextTurn();

    expect(oldConn.close).not.toHaveBeenCalled();
    expect(owner.connection.close).not.toHaveBeenCalled();
    expect(owner.reconnecting).toBe(false);
  });

  test('channels closed by a shutdown do not start a reconnect', async () => {
    const conn = fakeConnection();
    const owner = fakeOwner(conn);
    owner.isClose = true;

    rabbitMQCore.createOnChannelDead(owner, conn)('close');
    await nextTurn();

    expect(conn.close).not.toHaveBeenCalled();
  });

  test('a connection that fails to close is logged, not thrown', async () => {
    const warn = jest.spyOn(operationContext.global.logger, 'warn');
    const conn = fakeConnection();
    conn.close.mockRejectedValue(new Error('Connection closed'));
    const owner = fakeOwner(conn);

    rabbitMQCore.createOnChannelDead(owner, conn)('close');
    await nextTurn();
    await nextTurn();

    expect(conn.close).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith('[AMQP] close after channel %s: %s', 'close', expect.stringContaining('Connection closed'));
  });
});

describe('rabbitMQCore createChannelPromise', () => {
  test.each([
    ['createChannelPromise', 'createChannel'],
    ['createConfirmChannelPromise', 'createConfirmChannel']
  ])('%s reports a channel error, close and broker basic.cancel to the handler', async (helper, method) => {
    const conn = fakeConnection();
    const onChannelDead = jest.fn();

    const channel = await rabbitMQCore[helper](conn, onChannelDead);
    channel.emit('error', new Error('PRECONDITION_FAILED - unknown delivery tag 1'));
    channel.emit('cancel', {consumerTag: 'amq.ctag-1'});
    channel.emit('close');

    expect(conn[method]).toHaveBeenCalledTimes(1);
    expect(onChannelDead.mock.calls).toEqual([['error'], ['consumer cancelled by the broker'], ['close']]);
  });
});

describe('rabbitMQCore safeAck', () => {
  test('acks the message on the given channel', () => {
    const channel = {ack: jest.fn()};
    const message = {fields: {deliveryTag: 1}};

    expect(rabbitMQCore.safeAck(channel, message, 'task')).toBe(true);
    expect(channel.ack).toHaveBeenCalledWith(message);
  });

  test('a null message from a broker basic.cancel is not acked', () => {
    const channel = {ack: jest.fn()};

    expect(rabbitMQCore.safeAck(channel, null, 'pubsub')).toBe(false);
    expect(channel.ack).not.toHaveBeenCalled();
  });

  test('a cleared channel is skipped', () => {
    expect(rabbitMQCore.safeAck(null, {fields: {deliveryTag: 1}}, 'response')).toBe(false);
  });

  test('a closed channel does not throw', () => {
    const channel = {
      ack: jest.fn(() => {
        throw new amqp.IllegalOperationError('Channel closed');
      })
    };

    expect(rabbitMQCore.safeAck(channel, {fields: {deliveryTag: 1}}, 'dead')).toBe(false);
  });
});

describe('rabbitMQCore channel calls', () => {
  test('bindQueuePromise binds the queue and waits for the broker', async () => {
    const channel = {bindQueue: jest.fn().mockResolvedValue({})};

    await rabbitMQCore.bindQueuePromise(channel, 'ds.convertdead', 'ds.exchangeconvertdead', '');

    expect(channel.bindQueue).toHaveBeenCalledWith('ds.convertdead', 'ds.exchangeconvertdead', '', undefined);
  });

  test('checkQueuePromise resolves with the broker reply', async () => {
    const reply = {queue: 'ds.converttask', messageCount: 0, consumerCount: 1};
    const channel = {checkQueue: jest.fn().mockResolvedValue(reply)};

    await expect(rabbitMQCore.checkQueuePromise(channel, 'ds.converttask')).resolves.toEqual(reply);
  });

  test('assertQueuePromise and assertExchangePromise resolve with the name', async () => {
    const channel = {
      assertQueue: jest.fn().mockResolvedValue({queue: 'amq.gen-1', messageCount: 0, consumerCount: 0}),
      assertExchange: jest.fn().mockResolvedValue({exchange: 'ds.pubsub'})
    };

    await expect(rabbitMQCore.assertQueuePromise(channel, '', {autoDelete: true})).resolves.toBe('amq.gen-1');
    await expect(rabbitMQCore.assertExchangePromise(channel, 'ds.pubsub', 'fanout', {durable: true})).resolves.toBe('ds.pubsub');
  });

  test('a call on a closed channel rejects instead of throwing', async () => {
    const closed = () => {
      throw new amqp.IllegalOperationError('Channel closed');
    };
    const channel = {bindQueue: closed, checkQueue: closed, assertQueue: closed, close: closed};

    await expect(rabbitMQCore.bindQueuePromise(channel, 'q', 'x', '')).rejects.toThrow('Channel closed');
    await expect(rabbitMQCore.checkQueuePromise(channel, 'q')).rejects.toThrow('Channel closed');
    await expect(rabbitMQCore.assertQueuePromise(channel, 'q', {})).rejects.toThrow('Channel closed');
    await expect(rabbitMQCore.closePromise(channel)).rejects.toThrow('Channel closed');
  });
});

describe('rabbitMQCore runHealthCheck', () => {
  test('a check that resolves is healthy', async () => {
    await expect(rabbitMQCore.runHealthCheck(Promise.resolve({queue: 'ds.converttask'}), 5000, 'taskqueue healthCheck')).resolves.toBe(true);
  });

  test('a check that resolves to nothing is unhealthy', async () => {
    await expect(rabbitMQCore.runHealthCheck(Promise.resolve(''), 5000, 'pubsub healthCheck')).resolves.toBe(false);
  });

  test('a check that rejects is unhealthy', async () => {
    const check = Promise.reject(new Error("NOT_FOUND - no queue 'ds.converttask'"));

    await expect(rabbitMQCore.runHealthCheck(check, 5000, 'taskqueue healthCheck')).resolves.toBe(false);
  });

  test('a check with no reply is unhealthy after the timeout', async () => {
    jest.useFakeTimers();

    const result = rabbitMQCore.runHealthCheck(new Promise(() => {}), 5000, 'pubsub healthCheck');
    await jest.advanceTimersByTimeAsync(5000);

    await expect(result).resolves.toBe(false);
  });

  test('a check that settles in time leaves no timer behind', async () => {
    jest.useFakeTimers();

    await rabbitMQCore.runHealthCheck(Promise.resolve(true), 5000, 'pubsub healthCheck');

    expect(jest.getTimerCount()).toBe(0);
  });
});

describe('rabbitMQCore connetPromise', () => {
  test('retries until the broker accepts, then reports the connection close once', async () => {
    jest.useFakeTimers();
    const conn = fakeConnection();
    const connect = jest.spyOn(amqp, 'connect').mockRejectedValueOnce(new Error('connect ECONNREFUSED 127.0.0.1:5672')).mockResolvedValueOnce(conn);
    const closeCallback = jest.fn();

    const connected = rabbitMQCore.connetPromise(closeCallback);
    await jest.advanceTimersByTimeAsync(rabbitMQCore.RECONNECT_TIMEOUT);

    await expect(connected).resolves.toBe(conn);
    expect(connect).toHaveBeenCalledTimes(2);
    conn.emit('close');
    conn.emit('close');
    expect(closeCallback).toHaveBeenCalledTimes(1);
  });
});
