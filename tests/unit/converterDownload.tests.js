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

const {describe, test, expect, beforeEach, afterEach, jest} = require('@jest/globals');
const fs = require('fs');
const os = require('os');
const path = require('path');
const co = require('../../Common/node_modules/co');

const operationContext = require('../../Common/sources/operationContext');
const commonDefines = require('../../Common/sources/commondefines');
const constants = require('../../Common/sources/constants');
const storage = require('../../Common/sources/storage/storage-base');
const converter = require('../../FileConverter/sources/converter');

const ctx = new operationContext.Context();

// Lists like S3 and Azure do: every key that starts with the given string.
function fakeStorage(keys) {
  jest.spyOn(storage, 'listObjects').mockImplementation(async (_ctx, strPath) => Object.keys(keys).filter(key => key.startsWith(strPath)));
  jest.spyOn(storage, 'getObject').mockImplementation(async (_ctx, key) => Buffer.from(keys[key]));
}

function listFiles(dir) {
  return fs
    .readdirSync(dir, {recursive: true})
    .filter(name => fs.statSync(path.join(dir, name)).isFile())
    .sort();
}

describe('converter downloadFileFromStorage', () => {
  let dir;
  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'converter-download-'));
  });
  afterEach(() => {
    jest.restoreAllMocks();
    fs.rmSync(dir, {recursive: true, force: true});
  });

  test('a save key that another save of the document extends downloads only its own folder', async () => {
    // docservice names save folders docId + '_' + Math.round(Math.random() * 10000): "_57" and "_571" can coexist.
    fakeStorage({
      'doc/Editor.bin': 'editor',
      'doc/media/image1.png': 'image',
      'doc_57/Editor1.bin': 'this save',
      'doc_571/output.docx': 'an earlier save',
      'doc_571/changesHistory.json': '{}'
    });
    // ExecuteTask downloads the document, then processDownloadFromStorage its save folder, into the same dir.
    await co(converter.downloadFileFromStorage(ctx, 'doc', dir));
    const count = await co(converter.downloadFileFromStorage(ctx, 'doc_57', dir));

    expect(count).toBe(1);
    expect(listFiles(dir)).toEqual(['Editor.bin', 'Editor1.bin', path.join('media', 'image1.png')]);
    expect(fs.readFileSync(path.join(dir, 'Editor1.bin'), 'utf8')).toBe('this save');
  });

  test('keys outside the folder are skipped even when the storage returns them', async () => {
    fakeStorage({'doc_57/': '', 'doc_57/Editor1.bin': 'this save', 'doc_571/output.docx': 'an earlier save'});
    storage.listObjects.mockImplementation(async () => ['doc_57/', 'doc_57/Editor1.bin', 'doc_571/output.docx']);

    const count = await co(converter.downloadFileFromStorage(ctx, 'doc_57', dir));

    expect(count).toBe(1);
    expect(listFiles(dir)).toEqual(['Editor1.bin']);
  });
});

describe('converter ExecuteTask', () => {
  let tmpDir;
  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'converter-execute-'));
    jest.spyOn(os, 'tmpdir').mockReturnValue(tmpDir);
  });
  afterEach(() => {
    jest.restoreAllMocks();
    fs.rmSync(tmpDir, {recursive: true, force: true});
  });

  test('a task that throws removes its temp dir', async () => {
    jest.spyOn(storage, 'listObjects').mockImplementation(async () => {
      expect(fs.readdirSync(tmpDir)).toHaveLength(1); // the task's temp dir exists while it runs
      throw new Error('storage unavailable');
    });
    const cmd = new commonDefines.InputCommand();
    cmd.setDocId('doc');
    cmd.setSaveKey('_57');
    cmd.setOutputFormat(constants.AVS_OFFICESTUDIO_FILE_DOCUMENT_DOCX);
    const task = new commonDefines.TaskQueueData();
    task.setCmd(cmd);

    await expect(co(converter.ExecuteTask(ctx, task))).rejects.toThrow('storage unavailable');
    expect(storage.listObjects).toHaveBeenCalled();
    expect(fs.readdirSync(tmpDir)).toEqual([]);
  });
});
