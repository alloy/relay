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
const DEFAULT_DB_NAME = 'relay-runtime-rxdb-store';

type PutOperation = {type: 'put', dataID: DataID, record: ?Record};
type DeleteOperation = {type: 'remove', dataID: DataID};
type ClearOperation = {type: 'clear'};
type QueuedOperation = PutOperation | DeleteOperation | ClearOperation;

type RxDBModules = {
  createRxDatabase: (config: {...}) => Promise<any>,
  getRxStorageDexie: () => any,
  getRxStorageMemory: () => any,
};

/**
 * Experimental record source backed by RxDB with a bounded hot in-memory cache.
 * Writes are queued and flushed in bulk.
 */
class RelayRxDBRecordSource implements MutableRecordSource {
  _cacheSize: number;
  _dbName: string;
  _storage: 'dexie' | 'memory';
  _knownRecordStates: Map<DataID, RecordState>;
  _records: Map<DataID, ?Record>;
  _pendingOperations: Array<QueuedOperation>;
  _flushChain: Promise<void>;
  _flushScheduled: boolean;
  _rxdbModules: ?RxDBModules;
  _dbPromise: ?Promise<any>;
  _collectionPromise: ?Promise<any>;

  constructor(
    records?: RecordSourceJSON,
    options?: {
      cacheSize?: number,
      dbName?: string,
      storage?: 'dexie' | 'memory',
    },
  ) {
    this._cacheSize = options?.cacheSize ?? DEFAULT_CACHE_SIZE;
    this._dbName = options?.dbName ?? DEFAULT_DB_NAME;
    this._storage = options?.storage ?? 'dexie';
    this._knownRecordStates = new Map();
    this._records = new Map();
    this._pendingOperations = [];
    this._flushChain = Promise.resolve();
    this._flushScheduled = false;
    this._rxdbModules = RelayRxDBRecordSource._loadModules();
    this._dbPromise = null;
    this._collectionPromise = null;

    if (records != null) {
      Object.keys(records).forEach(dataID => {
        const object = records[dataID];
        const record = RelayModernRecord.fromObject<null | void>(object);
        if (record === undefined) {
          return;
        }
        this._knownRecordStates.set(dataID, record == null ? NONEXISTENT : EXISTENT);
        this._setCachedRecord(dataID, record);
        this._pendingOperations.push({type: 'put', dataID, record});
      });
      this._scheduleFlush();
    }
  }

  static isSupported(): boolean {
    return (
      typeof globalThis !== 'undefined' &&
      globalThis.indexedDB != null &&
      RelayRxDBRecordSource._loadModules() != null
    );
  }

  static create(
    records?: RecordSourceJSON,
    options?: {
      cacheSize?: number,
      dbName?: string,
      storage?: 'dexie' | 'memory',
    },
  ): MutableRecordSource {
    return new RelayRxDBRecordSource(records, options);
  }

  static _loadModules(): ?RxDBModules {
    try {
      const {createRxDatabase} = require('rxdb');
      const {getRxStorageDexie} = require('rxdb/plugins/storage-dexie');
      const {getRxStorageMemory} = require('rxdb/plugins/storage-memory');
      return {
        createRxDatabase,
        getRxStorageDexie,
        getRxStorageMemory,
      };
    } catch (_error) {
      return null;
    }
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
    if (!RelayRxDBRecordSource.isSupported()) {
      this._pendingOperations = [];
      return;
    }
    if (this._pendingOperations.length === 0) {
      return;
    }

    const operations = this._pendingOperations;
    this._pendingOperations = [];

    const lastClearIndex = operations.reduce(
      (acc, operation, index) => (operation.type === 'clear' ? index : acc),
      -1,
    );
    const pendingOps =
      lastClearIndex === -1 ? operations : operations.slice(lastClearIndex + 1);
    if (lastClearIndex !== -1) {
      await this._resetDatabase();
    }
    if (pendingOps.length === 0) {
      return;
    }
    const collection = await this._openCollection();
    await this._applyOperations(collection, pendingOps);
  }

  async prefetch(dataIDs: ReadonlyArray<DataID>): Promise<void> {
    if (!RelayRxDBRecordSource.isSupported() || dataIDs.length === 0) {
      return;
    }
    const collection = await this._openCollection();
    const docsByID = await this._runFindByIds(collection, Array.from(dataIDs));
    dataIDs.forEach(dataID => {
      const doc = docsByID.get(dataID);
      if (doc == null) {
        this._knownRecordStates.delete(dataID);
        this._records.delete(dataID);
        return;
      }
      const wrapped = doc.toJSON();
      const record = wrapped?.value;
      this._knownRecordStates.set(dataID, record == null ? NONEXISTENT : EXISTENT);
      this._setCachedRecord(dataID, record);
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
    const overflow = this._records.size - this._cacheSize;
    if (overflow <= 0) {
      return;
    }
    for (let ii = 0; ii < overflow; ii++) {
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

  async _openCollection(): Promise<any> {
    if (this._collectionPromise != null) {
      return this._collectionPromise;
    }
    if (this._rxdbModules == null) {
      throw new Error('RxDB modules unavailable');
    }
    this._collectionPromise = (async () => {
      const rxdbModules = this._rxdbModules;
      if (rxdbModules == null) {
        throw new Error('RxDB modules unavailable');
      }
      if (this._dbPromise == null) {
        const storage =
          this._storage === 'memory'
            ? rxdbModules.getRxStorageMemory()
            : rxdbModules.getRxStorageDexie();
        this._dbPromise = rxdbModules.createRxDatabase({
          name: this._dbName,
          storage,
          multiInstance: false,
          ignoreDuplicate: false,
        });
      }
      const db = await this._dbPromise;
      const collections = await db.addCollections({
        records: {
          schema: {
            title: 'relay-runtime-rxdb-records',
            version: 0,
            primaryKey: '__id',
            type: 'object',
            properties: {
              __id: {
                type: 'string',
                maxLength: 200,
              },
              value: {
                type: 'object',
                additionalProperties: true,
              },
            },
            required: ['__id', 'value'],
            additionalProperties: false,
          },
        },
      });
      return collections.records;
    })();
    return this._collectionPromise;
  }

  async _resetDatabase(): Promise<void> {
    if (this._dbPromise == null) {
      return;
    }
    const db = await this._dbPromise;
    await db.remove();
    this._dbPromise = null;
    this._collectionPromise = null;
  }

  async _applyOperations(
    collection: any,
    operations: ReadonlyArray<QueuedOperation>,
  ): Promise<void> {
    const putRows = [];
    const removeIDs = [];
    operations.forEach(operation => {
      if (operation.type === 'put') {
        putRows.push({
          __id: operation.dataID,
          value: operation.record,
        });
      } else if (operation.type === 'remove') {
        removeIDs.push(operation.dataID);
      }
    });

    if (putRows.length > 0) {
      await collection.bulkUpsert(putRows);
    }
    if (removeIDs.length > 0) {
      const docsByID = await this._runFindByIds(collection, removeIDs);
      await Promise.all(
        Array.from(docsByID.values()).map(doc => doc.remove()),
      );
    }
  }

  async _runFindByIds(
    collection: any,
    dataIDs: Array<DataID>,
  ): Promise<Map<string, any>> {
    const query = collection.findByIds(dataIDs);
    const result =
      query != null && typeof query.exec === 'function' ? await query.exec() : await query;
    if (result instanceof Map) {
      return result;
    }
    if (result != null && typeof result === 'object') {
      return new Map(Object.entries(result));
    }
    return new Map();
  }
}

module.exports = RelayRxDBRecordSource;
