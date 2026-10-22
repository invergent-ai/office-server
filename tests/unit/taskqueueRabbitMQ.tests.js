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

const rabbitMQCore = require('../../Common/sources/rabbitMQCore');
const TaskQueueRabbitMQ = require('../../Common/sources/taskqueueRabbitMQ');

const nextTurn = () => new Promise(resolve => setImmediate(resolve));

// A confirm channel that confirms every message except the ones named in `refused`, like a channel that closes before the confirm.
function confirmChannel(refused) {
  return {
    sendToQueue: jest.fn((_queue, content, _options, callback) => {
      setImmediate(() => callback(refused.includes(content.toString()) ? new Error('channel closed') : null));
    })
  };
}

function connectTo(channel) {
  jest.spyOn(rabbitMQCore, 'connetPromise').mockResolvedValue(new EventEmitter());
  jest.spyOn(rabbitMQCore, 'createConfirmChannelPromise').mockResolvedValue(channel);
  jest.spyOn(rabbitMQCore, 'assertQueuePromise').mockResolvedValue('queue');
}

const contents = store => store.map(entry => (Buffer.isBuffer(entry) ? entry : entry.task).toString());

afterEach(() => {
  jest.restoreAllMocks();
});

describe('taskqueueRabbitMQ replay on reconnect', () => {
  test('a buffered task whose confirm fails goes back to the store for the next reconnect', async () => {
    const channel = confirmChannel(['task b']);
    connectTo(channel);
    const queue = new TaskQueueRabbitMQ();
    queue.addTaskStore.push({task: Buffer.from('task a'), priority: 1}, {task: Buffer.from('task b'), priority: 1});

    await queue.initPromise(true, false, false, false, false, false);
    await nextTurn();

    expect(channel.sendToQueue).toHaveBeenCalledTimes(2);
    expect(contents(queue.addTaskStore)).toEqual(['task b']);
  });

  test('buffered responses and delayed tasks are kept the same way', async () => {
    connectTo(confirmChannel(['response b', 'delayed a']));
    const queue = new TaskQueueRabbitMQ();
    queue.addResponseStore.push(Buffer.from('response a'), Buffer.from('response b'));
    queue.addDelayedStore.push({task: Buffer.from('delayed a'), ttl: 1000}, {task: Buffer.from('delayed b'), ttl: 1000});

    await queue.initPromise(false, true, false, false, false, true);
    await nextTurn();

    expect(contents(queue.addResponseStore)).toEqual(['response b']);
    expect(contents(queue.addDelayedStore)).toEqual(['delayed a']);
  });

  test('a store with no channel to send on keeps every entry', async () => {
    connectTo(confirmChannel([]));
    const queue = new TaskQueueRabbitMQ();
    queue.addTaskStore.push({task: Buffer.from('task a'), priority: 1}, {task: Buffer.from('task b'), priority: 1});

    // this process sends no tasks, so channelConvertTask stays null and every send fails at once
    await queue.initPromise(false, true, false, false, false, false);

    expect(contents(queue.addTaskStore)).toEqual(['task a', 'task b']);
  });
});
