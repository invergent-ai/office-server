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

const operationContext = require('../../Common/sources/operationContext');
const commonDefines = require('../../Common/sources/commondefines');
const constants = require('../../Common/sources/constants');
const docsCoServer = require('../../DocService/sources/DocsCoServer');
const canvasService = require('../../DocService/sources/canvasservice');
const converterService = require('../../DocService/sources/converterservice');
const taskResult = require('../../DocService/sources/taskresult');

const ctx = new operationContext.Context();
const {Command, Button, Timeout, Form, Internal} = commonDefines.c_oAscForceSaveTypes;

// The priority and expiration startForceSave gives the conversion of a forcesave of this type.
async function forceSaveQueueing(type) {
  const docId = `doc-${type}`;
  await docsCoServer.editorData.setForceSave(ctx, docId, 1000, 1, 'http://docs', {user_id: 'u'}, null);
  jest.spyOn(taskResult, 'selectWithCache').mockResolvedValue([{callback: 'http://callback'}]);
  jest.spyOn(converterService, 'convertFromChanges').mockResolvedValue(new commonDefines.ConvertStatus(constants.NO_ERROR));
  await docsCoServer.startForceSave(ctx, docId, type);
  const [call] = converterService.convertFromChanges.mock.calls;
  return {priority: call[10], expiration: call[11]}; // opt_priority, opt_expiration
}

// The priority saveFromChanges queues a closing save at.
async function closingSavePriority() {
  jest
    .spyOn(taskResult, 'select')
    .mockResolvedValue([
      {status: commonDefines.FileStatus.SaveVersion, status_info: 7, callback: '', additional: null, password: null, change_id: 0}
    ]);
  // eslint-disable-next-line require-yield
  jest.spyOn(taskResult, 'addRandomKeyTask').mockImplementation(function* () {
    return {key: 'doc-closing_57'};
  });
  jest.spyOn(docsCoServer, 'addTask').mockImplementation(function* () {});
  await canvasService.saveFromChanges(ctx, 'doc-closing', 7);
  return docsCoServer.addTask.mock.calls[0][1];
}

describe('conversion queue priority of saves', () => {
  afterEach(() => jest.restoreAllMocks());

  test('a forcesave someone waits on is queued no lower than a closing save, and below a document open', async () => {
    const closing = await closingSavePriority();
    expect(closing).toBe(constants.QUEUE_PRIORITY_NORMAL);
    for (const type of [Command, Button, Form, Internal]) {
      jest.restoreAllMocks();
      const {priority, expiration} = await forceSaveQueueing(type);
      expect(priority).toBeGreaterThanOrEqual(closing);
      expect(priority).toBeLessThan(constants.QUEUE_PRIORITY_HIGH);
      expect(expiration).toBeUndefined();
    }
  });

  test('a timed save stays below a closing save and keeps its expiration', async () => {
    const closing = await closingSavePriority();
    jest.restoreAllMocks();
    const {priority, expiration} = await forceSaveQueueing(Timeout);
    expect(priority).toBe(constants.QUEUE_PRIORITY_VERY_LOW);
    expect(priority).toBeLessThan(closing);
    expect(expiration).toBeGreaterThan(0);
  });
});
