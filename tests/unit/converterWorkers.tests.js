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

const {describe, test, expect} = require('@jest/globals');
const {getWorkersCount} = require('../../FileConverter/sources/converterWorkers');

describe('converter getWorkersCount', () => {
  test('a set number of workers is used whatever the CPUs the process sees', () => {
    expect(getWorkersCount(4, 1, 56)).toBe(4);
    expect(getWorkersCount(4, 0.001, 6)).toBe(4);
    expect(getWorkersCount(4, 1, 2)).toBe(4);
    expect(getWorkersCount(1, 1, 56)).toBe(1);
  });

  test('with no number set, the workers follow the CPUs as upstream', () => {
    expect(getWorkersCount(0, 1, 8)).toBe(8);
    expect(getWorkersCount(0, 0.5, 6)).toBe(3);
    expect(getWorkersCount(0, 0.001, 56)).toBe(1);
  });

  test('a value that is not 0 or a whole number of at least 1 is refused', () => {
    for (const bad of [-1, 2.5, '4', null, NaN]) {
      expect(() => getWorkersCount(bad, 1, 8)).toThrow('FileConverter.converter.workers must be 0 or a whole number of at least 1');
    }
  });
});
