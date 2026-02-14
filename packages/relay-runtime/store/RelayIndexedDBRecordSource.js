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

import type {DataID} from '../util/RelayRuntimeTypes';
import type {RecordState} from './RelayRecordState';
import type {
  MutableRecordSource,
  Record,
  RecordSourceJSON,
} from './RelayStoreTypes';

const RelayFeatureFlags = require('../util/RelayFeatureFlags');
const RelayModernRecord = require('./RelayModernRecord');
const RelayRecordState = require('./RelayRecordState');
const {RELAY_RESOLVER_RECORD_TYPENAME} = require('./RelayStoreUtils');

const {EXISTENT, NONEXISTENT, UNKNOWN} = RelayRecordState;
const DEFAULT_CACHE_SIZE = 1000;
const DEFAULT_DB_NAME = 'relay-runtime-store';
const DEFAULT_OBJECT_STORE_NAME = 'records';

type PutOperation = {type: 'put', dataID: DataID, record: ?Record};
type DeleteOperation = {type: 'remove', dataID: DataID};
type ClearOperation = {type: 'clear'};
type QueuedOperation = PutOperation | DeleteOperation | ClearOperation;

/**
 * Experimental record source backed by IndexedDB with a bounded hot in-memory
 * cache. Writes are queued and flushed in a single transaction.
 */
class RelayIndexedDBRecordSource implements MutableRecordSource {
  _cacheSize: number;
  _dbName: string;
  _objectStoreName: string;
  _knownRecordStates: Map<DataID, RecordState>;
  _records: Map<DataID, ?Record>;
  _pendingOperations: Array<QueuedOperation>;
  _flushChain: Promise<void>;
  _flushScheduled: boolean;
  _dbPromise: ?Promise<any>;

  constructor(
    records?: RecordSourceJSON,
    options?: {
      cacheSize?: number,
      dbName?: string,
      objectStoreName?: string,
    },
  ) {
    this._cacheSize = options?.cacheSize ?? DEFAULT_CACHE_SIZE;
    this._dbName = options?.dbName ?? DEFAULT_DB_NAME;
    this._objectStoreName = options?.objectStoreName ?? DEFAULT_OBJECT_STORE_NAME;
    this._knownRecordStates = new Map();
    this._records = new Map();
    this._pendingOperations = [];
    this._flushChain = Promise.resolve();
    this._flushScheduled = false;
    this._dbPromise = null;

    if (records != null) {
      Object.keys(records).forEach(dataID => {
        const object = records[dataID];
        const record = RelayModernRecord.fromObject<null | void>(object);
        if (record === undefined) {
          return;
        }
        const state = record == null ? NONEXISTENT : EXISTENT;
        this._knownRecordStates.set(dataID, state);
        this._setCachedRecord(dataID, record);
        this._pendingOperations.push({type: 'put', dataID, record});
      });
      this._scheduleFlush();
    }
  }

  static isSupported(): boolean {
    return typeof globalThis !== 'undefined' && globalThis.indexedDB != null;
  }

  static create(
    records?: RecordSourceJSON,
    options?: {
      cacheSize?: number,
      dbName?: string,
      objectStoreName?: string,
    },
  ): MutableRecordSource {
    return new RelayIndexedDBRecordSource(records, options);
  }

  clear(): void {
    this._knownRecordStates.clear();
    this._records.clear();
    this._pendingOperations.push({type: 'clear'});
    this._scheduleFlush();
  }

  delete(dataID: DataID): void {
    this._knownRecordStates.set(dataID, NONEXISTENT);
    this._setCachedRecord(dataID, null);
    this._pendingOperations.push({type: 'put', dataID, record: null});
    this._scheduleFlush();
  }

  get(dataID: DataID): ?Record {
    const cachedRecord = this._records.get(dataID);
    if (cachedRecord === undefined) {
      return undefined;
    }
    this._touch(dataID, cachedRecord);
    return cachedRecord;
  }

  getRecordIDs(): Array<DataID> {
    return Array.from(this._knownRecordStates.keys());
  }

  getStatus(dataID: DataID): RecordState {
    return this._knownRecordStates.get(dataID) ?? UNKNOWN;
  }

  has(dataID: DataID): boolean {
    return this._knownRecordStates.has(dataID);
  }

  remove(dataID: DataID): void {
    this._knownRecordStates.delete(dataID);
    this._records.delete(dataID);
    this._pendingOperations.push({type: 'remove', dataID});
    this._scheduleFlush();
  }

  set(dataID: DataID, record: Record): void {
    this._knownRecordStates.set(dataID, EXISTENT);
    this._setCachedRecord(dataID, record);
    this._pendingOperations.push({type: 'put', dataID, record});
    this._scheduleFlush();
  }

  size(): number {
    return this._knownRecordStates.size;
  }

  toJSON(): RecordSourceJSON {
    const obj: {...RecordSourceJSON} = {};
    this._records.forEach((record, key) => {
      if (
        RelayFeatureFlags.FILTER_OUT_RELAY_RESOLVER_RECORDS &&
        record != null &&
        RelayModernRecord.getType(record) === RELAY_RESOLVER_RECORD_TYPENAME
      ) {
        return;
      }
      obj[key] = RelayModernRecord.toJSON<null | void>(record);
    });
    return obj;
  }

  async flush(): Promise<void> {
    if (!RelayIndexedDBRecordSource.isSupported()) {
      this._pendingOperations = [];
      return;
    }
    if (this._pendingOperations.length === 0) {
      return;
    }

    const operations = this._pendingOperations;
    this._pendingOperations = [];
    const db = await this._openDB();
    await this._runReadWriteTransaction(db, operations);
  }

  async prefetch(dataIDs: ReadonlyArray<DataID>): Promise<void> {
    if (!RelayIndexedDBRecordSource.isSupported() || dataIDs.length === 0) {
      return;
    }
    const db = await this._openDB();
    await new Promise((resolve, reject) => {
      const tx = db.transaction([this._objectStoreName], 'readonly');
      const store = tx.objectStore(this._objectStoreName);
      let remaining = dataIDs.length;
      if (remaining === 0) {
        resolve();
        return;
      }
      dataIDs.forEach(dataID => {
        const request = store.get(dataID);
        request.onerror = () => {
          reject(request.error);
        };
        request.onsuccess = () => {
          const record = request.result;
          if (record === undefined) {
            this._knownRecordStates.delete(dataID);
            this._records.delete(dataID);
          } else {
            this._knownRecordStates.set(
              dataID,
              record == null ? NONEXISTENT : EXISTENT,
            );
            this._setCachedRecord(dataID, record);
          }
          remaining--;
          if (remaining === 0) {
            resolve();
          }
        };
      });
    });
  }

  _scheduleFlush(): void {
    if (this._flushScheduled) {
      return;
    }
    this._flushScheduled = true;
    const schedule = typeof setTimeout === 'function' ? setTimeout : null;
    if (schedule == null) {
      this._flushScheduled = false;
      return;
    }
    schedule(() => {
      this._flushScheduled = false;
      this._flushChain = this._flushChain
        .then(() => this.flush())
        .catch(() => {
          // keep queueing future flushes after failures
        });
    }, 0);
  }

  _setCachedRecord(dataID: DataID, record: ?Record): void {
    this._records.set(dataID, record);
    this._touch(dataID, record);
    while (this._records.size > this._cacheSize) {
      const firstKey = this._records.keys().next().value;
      if (firstKey == null) {
        break;
      }
      this._records.delete(firstKey);
    }
  }

  _touch(dataID: DataID, record: ?Record): void {
    if (!this._records.has(dataID)) {
      return;
    }
    this._records.delete(dataID);
    this._records.set(dataID, record);
  }

  _openDB(): Promise<any> {
    if (this._dbPromise == null) {
      this._dbPromise = new Promise((resolve, reject) => {
        const request = globalThis.indexedDB.open(this._dbName, 1);
        request.onupgradeneeded = event => {
          const db = event.target.result;
          if (!db.objectStoreNames.contains(this._objectStoreName)) {
            db.createObjectStore(this._objectStoreName);
          }
        };
        request.onerror = () => reject(request.error);
        request.onsuccess = () => resolve(request.result);
      });
    }
    return this._dbPromise;
  }

  _runReadWriteTransaction(
    db: any,
    operations: ReadonlyArray<QueuedOperation>,
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      const tx = db.transaction([this._objectStoreName], 'readwrite');
      const store = tx.objectStore(this._objectStoreName);
      operations.forEach(operation => {
        if (operation.type === 'clear') {
          store.clear();
        } else if (operation.type === 'remove') {
          store.delete(operation.dataID);
        } else {
          store.put(operation.record, operation.dataID);
        }
      });
      tx.onerror = () => reject(tx.error);
      tx.oncomplete = () => resolve();
    });
  }
}

module.exports = RelayIndexedDBRecordSource;
