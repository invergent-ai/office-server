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

'use strict';

/**
 * The number of converter workers to run.
 * When workers is set (a whole number of at least 1), the converter runs exactly that many,
 * whatever the CPUs the process sees. When it is 0, it runs upstream's number:
 * the fraction maxProcessCount of the CPUs the process sees, rounded up.
 * @param {number} workers - FileConverter.converter.workers
 * @param {number} maxProcessCount - FileConverter.converter.maxprocesscount
 * @param {number} parallelism - the CPUs the process sees
 * @returns {number}
 */
function getWorkersCount(workers, maxProcessCount, parallelism) {
  if (Number.isInteger(workers) && workers >= 1) {
    return workers;
  }
  if (workers === 0) {
    return Math.ceil(parallelism * maxProcessCount);
  }
  const value = typeof workers === 'string' ? JSON.stringify(workers) : workers;
  throw new Error(`FileConverter.converter.workers must be 0 or a whole number of at least 1, not ${value}`);
}

module.exports = {getWorkersCount};
