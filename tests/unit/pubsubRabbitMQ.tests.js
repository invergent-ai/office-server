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
const rabbitMQCore = require('../../Common/sources/rabbitMQCore');
const PubsubRabbitMQ = require('../../DocService/sources/pubsubRabbitMQ');

// A publish channel that takes every message except the ones named in `refused`, like a channel that has just closed.
function publishChannel(refused) {
  const channel = new EventEmitter();
  channel.publish = jest.fn((_exchange, _routingKey, data) => {
    if (refused.includes(data.toString())) {
      throw new amqp.IllegalOperationError('Channel closed');
    }
    return true;
  });
  return channel;
}
// The messages the channel took; a refused publish throws.
const published = channel =>
  channel.publish.mock.calls.filter((_call, i) => 'return' === channel.publish.mock.results[i].type).map(call => call[2].toString());

// Each init connects and opens a publish channel, then a receive channel.
function connectTo(...channels) {
  jest.spyOn(rabbitMQCore, 'connetPromise').mockResolvedValue(new EventEmitter());
  const createChannel = jest.spyOn(rabbitMQCore, 'createChannelPromise');
  for (const channel of channels) {
    createChannel.mockResolvedValueOnce(channel).mockResolvedValueOnce(new EventEmitter());
  }
  jest.spyOn(rabbitMQCore, 'assertExchangePromise').mockResolvedValue('ds.pubsub');
  jest.spyOn(rabbitMQCore, 'assertQueuePromise').mockResolvedValue('amq.gen-1');
  jest.spyOn(rabbitMQCore, 'bindQueuePromise').mockResolvedValue({});
  jest.spyOn(rabbitMQCore, 'consumePromise').mockResolvedValue({consumerTag: 'amq.ctag-1'});
}

afterEach(() => {
  jest.restoreAllMocks();
});

describe('pubsubRabbitMQ publish buffering', () => {
  test('a message published while disconnected is published on connect', async () => {
    const channel = publishChannel([]);
    connectTo(channel);
    const pubsub = new PubsubRabbitMQ();

    await pubsub.publish('before connect');
    await pubsub.initPromise();

    expect(published(channel)).toEqual(['before connect']);
    expect(pubsub.publishStore).toEqual([]);
  });

  test('a message a closed channel refuses is kept and published by the next reconnect', async () => {
    const closedChannel = publishChannel(['shutdown signal']);
    const nextChannel = publishChannel([]);
    connectTo(closedChannel, nextChannel);
    const pubsub = new PubsubRabbitMQ();
    await pubsub.initPromise();

    await pubsub.publish('shutdown signal');
    expect(pubsub.publishStore.map(String)).toEqual(['shutdown signal']);
    await pubsub.initPromise();

    expect(published(nextChannel)).toEqual(['shutdown signal']);
    expect(pubsub.publishStore).toEqual([]);
  });

  test('the replay keeps the messages the channel refuses', async () => {
    const channel = publishChannel(['message b']);
    connectTo(channel);
    const pubsub = new PubsubRabbitMQ();
    await pubsub.publish('message a');
    await pubsub.publish('message b');

    await pubsub.initPromise();

    expect(published(channel)).toEqual(['message a']);
    expect(pubsub.publishStore.map(String)).toEqual(['message b']);
  });

  test('the store keeps the newest 1000000 messages', async () => {
    const pubsub = new PubsubRabbitMQ();
    pubsub.publishStore = new Array(1000000).fill(Buffer.from('old'));
    pubsub.publishStore[0] = Buffer.from('oldest');

    await pubsub.publish('newest');

    expect(pubsub.publishStore).toHaveLength(1000000);
    expect(pubsub.publishStore[0].toString()).toBe('old');
    expect(pubsub.publishStore[999999].toString()).toBe('newest');
  });
});
