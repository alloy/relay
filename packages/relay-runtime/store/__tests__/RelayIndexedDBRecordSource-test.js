/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 * @flow strict-local
 * @format
 * @oncall relay
 */

'use strict';

const RelayIndexedDBRecordSource = require('../RelayIndexedDBRecordSource');
const RelayModernRecord = require('../RelayModernRecord');

describe('RelayIndexedDBRecordSource', () => {
  const originalIndexedDB = global.indexedDB;

  afterEach(() => {
    global.indexedDB = originalIndexedDB;
  });

  it('supports basic set/get/delete/remove semantics without IndexedDB', async () => {
    global.indexedDB = undefined;
    const source = new RelayIndexedDBRecordSource();
    const record = RelayModernRecord.create('user:1', 'User');

    source.set('user:1', record);
    expect(source.get('user:1')).toBe(record);
    expect(source.getStatus('user:1')).toBe('EXISTENT');

    source.delete('user:1');
    expect(source.get('user:1')).toBe(null);
    expect(source.getStatus('user:1')).toBe('NONEXISTENT');

    source.remove('user:1');
    expect(source.get('user:1')).toBe(undefined);
    expect(source.getStatus('user:1')).toBe('UNKNOWN');

    await source.flush();
    await source.prefetch(['user:1']);
  });

  it('enforces a bounded hot cache', () => {
    global.indexedDB = undefined;
    const source = new RelayIndexedDBRecordSource(undefined, {cacheSize: 1});
    const record1 = RelayModernRecord.create('user:1', 'User');
    const record2 = RelayModernRecord.create('user:2', 'User');

    source.set('user:1', record1);
    source.set('user:2', record2);

    expect(source.has('user:1')).toBe(true);
    expect(source.has('user:2')).toBe(true);
    expect(source.get('user:1')).toBe(undefined);
    expect(source.get('user:2')).toBe(record2);
  });
});
