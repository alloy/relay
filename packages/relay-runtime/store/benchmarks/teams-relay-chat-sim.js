import React, {useMemo, useState} from 'react';
import {createRoot} from 'react-dom/client';
import {RecordSource, Store} from 'relay-runtime';
import {createRxDatabase} from 'rxdb';
import {getRxStorageDexie} from 'rxdb/plugins/storage-dexie';

const DB_VERSION = 1;
const IDB_DB_NAME = 'relay-teams-chat-idb';
const IDB_STORE = 'records';

function now() {
  return performance.now();
}

function heapBytes() {
  return performance?.memory?.usedJSHeapSize ?? null;
}

function p95(values) {
  if (values.length === 0) {
    return 0;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(
    sorted.length - 1,
    Math.floor(sorted.length * 0.95),
  );
  return sorted[index];
}

function makeTeamsDataset(chatCount, messagesPerChat) {
  const records = {};
  const chatIDs = [];
  const messageIDsByChat = new Map();
  for (let chatIndex = 0; chatIndex < chatCount; chatIndex++) {
    const chatID = `chat:${chatIndex}`;
    chatIDs.push(chatID);
    const messageIDs = [];
    for (
      let messageIndex = 0;
      messageIndex < messagesPerChat;
      messageIndex++
    ) {
      const messageID = `${chatID}:message:${messageIndex}`;
      messageIDs.push(messageID);
      records[messageID] = {
        __id: messageID,
        __typename: 'Message',
        id: messageID,
        chatID,
        author:
          messageIndex % 3 === 0
            ? 'Avery'
            : messageIndex % 3 === 1
              ? 'Jordan'
              : 'Riley',
        body: `Message ${messageIndex} in ${chatID}`,
        sentAt: messageIndex,
      };
    }
    messageIDsByChat.set(chatID, messageIDs);
    records[chatID] = {
      __id: chatID,
      __typename: 'Chat',
      id: chatID,
      title: `Engineering ${chatIndex + 1}`,
      messageIDs,
    };
  }
  return {records, chatIDs, messageIDsByChat};
}

class InMemoryChatSource {
  constructor(records) {
    this._recordSource = RecordSource.create(records);
    this._relayStore = new Store(this._recordSource);
    void this._relayStore;
  }

  async loadChat(messageIDs) {
    return messageIDs.map(id => this._recordSource.get(id)).filter(Boolean);
  }

  async dispose() {}
}

class IndexedDBChatSource {
  constructor(records) {
    this._recordSource = RecordSource.create();
    this._relayStore = new Store(this._recordSource);
    void this._relayStore;
    this._records = records;
    this._dbPromise = null;
  }

  async init() {
    const db = await this._openDB();
    await new Promise((resolve, reject) => {
      const tx = db.transaction([IDB_STORE], 'readwrite');
      const store = tx.objectStore(IDB_STORE);
      Object.keys(this._records).forEach(id => store.put(this._records[id], id));
      tx.onerror = () => reject(tx.error);
      tx.oncomplete = () => resolve();
    });
  }

  async loadChat(messageIDs) {
    const db = await this._openDB();
    await new Promise((resolve, reject) => {
      const tx = db.transaction([IDB_STORE], 'readonly');
      const store = tx.objectStore(IDB_STORE);
      let remaining = messageIDs.length;
      messageIDs.forEach(id => {
        const request = store.get(id);
        request.onerror = () => reject(request.error);
        request.onsuccess = () => {
          if (request.result != null) {
            this._recordSource.set(id, request.result);
          }
          remaining--;
          if (remaining === 0) {
            resolve();
          }
        };
      });
      if (messageIDs.length === 0) {
        resolve();
      }
    });
    return messageIDs.map(id => this._recordSource.get(id)).filter(Boolean);
  }

  async dispose() {
    if (this._dbPromise != null) {
      const db = await this._dbPromise;
      db.close();
    }
  }

  async _openDB() {
    if (this._dbPromise != null) {
      return this._dbPromise;
    }
    this._dbPromise = new Promise((resolve, reject) => {
      const request = indexedDB.open(IDB_DB_NAME, DB_VERSION);
      request.onupgradeneeded = event => {
        const db = event.target?.result;
        if (db != null && !db.objectStoreNames.contains(IDB_STORE)) {
          db.createObjectStore(IDB_STORE);
        }
      };
      request.onerror = () => reject(request.error);
      request.onsuccess = () => resolve(request.result);
    });
    return this._dbPromise;
  }
}

class RxDBChatSource {
  constructor(records) {
    this._recordSource = RecordSource.create();
    this._relayStore = new Store(this._recordSource);
    void this._relayStore;
    this._records = records;
    this._db = null;
    this._collection = null;
  }

  async init() {
    this._db = await createRxDatabase({
      name: `relay-teams-rxdb-${Date.now().toString(36)}${Math.floor(Math.random() * 1000)}`,
      storage: getRxStorageDexie(),
      multiInstance: false,
    });
    const collections = await this._db.addCollections({
      records: {
        schema: {
          title: 'relay teams records',
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
    this._collection = collections.records;
    const wrapped = Object.keys(this._records).map(id => ({
      __id: id,
      value: this._records[id],
    }));
    await this._collection.bulkUpsert(wrapped);
  }

  async loadChat(messageIDs) {
    const query = this._collection.findByIds(messageIDs);
    const docsByIDRaw =
      query != null && typeof query.exec === 'function'
        ? await query.exec()
        : await query;
    const docsByID =
      docsByIDRaw instanceof Map
        ? docsByIDRaw
        : new Map(Object.entries(docsByIDRaw ?? {}));
    messageIDs.forEach(id => {
      const doc = docsByID.get(id);
      if (doc != null) {
        this._recordSource.set(id, doc.toJSON().value);
      }
    });
    return messageIDs.map(id => this._recordSource.get(id)).filter(Boolean);
  }

  async dispose() {
    if (this._db != null) {
      await this._db.remove();
    }
  }
}

async function runSwitchBenchmark({
  caseName,
  createSource,
  chatIDs,
  messageIDsByChat,
  switchCount,
}) {
  const source = await createSource();
  const startHeap = heapBytes();
  const latencies = [];
  const totalStart = now();
  for (let ii = 0; ii < switchCount; ii++) {
    const chatID = chatIDs[ii % chatIDs.length];
    const messageIDs = messageIDsByChat.get(chatID) ?? [];
    const start = now();
    // eslint-disable-next-line no-await-in-loop
    await source.loadChat(messageIDs);
    latencies.push(now() - start);
  }
  const totalMs = now() - totalStart;
  const endHeap = heapBytes();
  await source.dispose();
  return {
    caseName,
    switchCount,
    avgSwitchMs:
      latencies.reduce((sum, value) => sum + value, 0) /
      Math.max(latencies.length, 1),
    p95SwitchMs: p95(latencies),
    maxSwitchMs: Math.max(...latencies),
    totalMs,
    heapDeltaBytes:
      startHeap == null || endHeap == null ? null : endHeap - startHeap,
  };
}

function TeamsChatSimulationApp() {
  const [chatCount, setChatCount] = useState(24);
  const [messagesPerChat, setMessagesPerChat] = useState(80);
  const [switchCount, setSwitchCount] = useState(120);
  const [running, setRunning] = useState(false);
  const [results, setResults] = useState([]);
  const [selectedChatIndex, setSelectedChatIndex] = useState(0);

  const dataset = useMemo(
    () => makeTeamsDataset(chatCount, messagesPerChat),
    [chatCount, messagesPerChat],
  );
  const selectedChatID = dataset.chatIDs[selectedChatIndex] ?? dataset.chatIDs[0];
  const selectedMessages = (dataset.messageIDsByChat.get(selectedChatID) ?? [])
    .slice(0, 16)
    .map(id => dataset.records[id]);

  async function onRun() {
    setRunning(true);
    try {
      const nextResults = [];
      nextResults.push(
        await runSwitchBenchmark({
          caseName: 'relay-in-memory-store',
          createSource: async () => new InMemoryChatSource(dataset.records),
          chatIDs: dataset.chatIDs,
          messageIDsByChat: dataset.messageIDsByChat,
          switchCount,
        }),
      );
      nextResults.push(
        await runSwitchBenchmark({
          caseName: 'relay-indexeddb-store',
          createSource: async () => {
            const source = new IndexedDBChatSource(dataset.records);
            await source.init();
            return source;
          },
          chatIDs: dataset.chatIDs,
          messageIDsByChat: dataset.messageIDsByChat,
          switchCount,
        }),
      );
      nextResults.push(
        await runSwitchBenchmark({
          caseName: 'relay-rxdb-store',
          createSource: async () => {
            const source = new RxDBChatSource(dataset.records);
            await source.init();
            return source;
          },
          chatIDs: dataset.chatIDs,
          messageIDsByChat: dataset.messageIDsByChat,
          switchCount,
        }),
      );
      setResults(nextResults);
    } finally {
      setRunning(false);
    }
  }

  return React.createElement(
    'div',
    {
      style: {
        minHeight: '100vh',
        display: 'grid',
        gridTemplateRows: '56px 1fr',
      },
    },
    React.createElement(
      'header',
      {
        style: {
          background: '#464775',
          color: 'white',
          display: 'flex',
          alignItems: 'center',
          padding: '0 16px',
          fontWeight: 700,
          fontSize: 20,
        },
      },
      'Teams-like Relay Chat Performance Lab',
    ),
    React.createElement(
      'main',
      {
        style: {
          display: 'grid',
          gridTemplateColumns: '280px 1fr 420px',
          gap: 12,
          padding: 12,
        },
      },
      React.createElement(
        'section',
        {style: {background: '#ffffff', border: '1px solid #ddd', padding: 12}},
        React.createElement('h3', {style: {marginTop: 0}}, 'Chats'),
        dataset.chatIDs.slice(0, 30).map((chatID, index) =>
          React.createElement(
            'button',
            {
              key: chatID,
              type: 'button',
              onClick: () => setSelectedChatIndex(index),
              style: {
                width: '100%',
                textAlign: 'left',
                padding: '8px 10px',
                border: 'none',
                background: index === selectedChatIndex ? '#e6f2ff' : 'transparent',
                cursor: 'pointer',
              },
            },
            dataset.records[chatID].title,
          ),
        ),
      ),
      React.createElement(
        'section',
        {style: {background: '#ffffff', border: '1px solid #ddd', padding: 12}},
        React.createElement('h3', {style: {marginTop: 0}}, `Conversation: ${selectedChatID}`),
        selectedMessages.map(message =>
          React.createElement(
            'div',
            {
              key: message.__id,
              style: {
                borderBottom: '1px solid #efefef',
                padding: '8px 0',
              },
            },
            React.createElement('div', {style: {fontWeight: 600}}, message.author),
            React.createElement('div', null, message.body),
          ),
        ),
      ),
      React.createElement(
        'section',
        {style: {background: '#ffffff', border: '1px solid #ddd', padding: 12}},
        React.createElement('h3', {style: {marginTop: 0}}, 'Simulation controls'),
        React.createElement(
          'label',
          null,
          'Chats: ',
          React.createElement('input', {
            type: 'number',
            value: chatCount,
            min: 4,
            onChange: e => setChatCount(Number(e.target.value)),
          }),
        ),
        React.createElement('br'),
        React.createElement(
          'label',
          null,
          'Messages/chat: ',
          React.createElement('input', {
            type: 'number',
            value: messagesPerChat,
            min: 10,
            onChange: e => setMessagesPerChat(Number(e.target.value)),
          }),
        ),
        React.createElement('br'),
        React.createElement(
          'label',
          null,
          'Switches: ',
          React.createElement('input', {
            type: 'number',
            value: switchCount,
            min: 20,
            onChange: e => setSwitchCount(Number(e.target.value)),
          }),
        ),
        React.createElement('br'),
        React.createElement(
          'button',
          {
            type: 'button',
            disabled: running,
            onClick: onRun,
            style: {marginTop: 10},
          },
          running ? 'Running...' : 'Run chat switch simulation',
        ),
        React.createElement('h4', null, 'Results'),
        React.createElement(
          'pre',
          {
            style: {
              whiteSpace: 'pre-wrap',
              fontSize: 12,
              background: '#f8f8f8',
              padding: 8,
              border: '1px solid #eee',
              minHeight: 220,
            },
          },
          JSON.stringify(results, null, 2),
        ),
      ),
    ),
  );
}

const rootEl = document.getElementById('root');
if (rootEl == null) {
  throw new Error('Root element not found');
}
const root = createRoot(rootEl);
root.render(React.createElement(TeamsChatSimulationApp));
