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

const RelayModernRecord = require('../RelayModernRecord');
const RelayRxDBRecordSource = require('../RelayRxDBRecordSource');

describe('RelayRxDBRecordSource', () => {
  const originalIndexedDB = global.indexedDB;

  afterEach(() => {
    global.indexedDB = originalIndexedDB;
  });

  it('handles set/get/delete/remove semantics without RxDB support', async () => {
    global.indexedDB = undefined;
    const source = new RelayRxDBRecordSource();
    const record = RelayModernRecord.create('chat:1', 'Chat');

    source.set('chat:1', record);
    expect(source.get('chat:1')).toBe(record);
    expect(source.getStatus('chat:1')).toBe('EXISTENT');

    source.delete('chat:1');
    expect(source.get('chat:1')).toBe(null);
    expect(source.getStatus('chat:1')).toBe('NONEXISTENT');

    source.remove('chat:1');
    expect(source.get('chat:1')).toBe(undefined);
    expect(source.getStatus('chat:1')).toBe('UNKNOWN');

    await source.flush();
    await source.prefetch(['chat:1']);
  });

  it('enforces a bounded hot cache', () => {
    global.indexedDB = undefined;
    const source = new RelayRxDBRecordSource(undefined, {cacheSize: 1});
    const record1 = RelayModernRecord.create('chat:1', 'Chat');
    const record2 = RelayModernRecord.create('chat:2', 'Chat');

    source.set('chat:1', record1);
    source.set('chat:2', record2);

    expect(source.has('chat:1')).toBe(true);
    expect(source.has('chat:2')).toBe(true);
    expect(source.get('chat:1')).toBe(undefined);
    expect(source.get('chat:2')).toBe(record2);
  });
});
