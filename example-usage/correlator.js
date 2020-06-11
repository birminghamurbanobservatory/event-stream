const cls = require('cls-hooked');
const shortId = require('shortid');

// Based on: https://medium.com/@evgeni.kisel/add-correlation-id-in-node-js-applications-fde759eed5e3

const store = cls.createNamespace(`correlation-id-namespace`);

const CORRELATION_ID_KEY = `correlation-id`;

// executes specified function with correlation ID. If ID is missing then new ID is generated
async function withCorrelationId(fn, id) {
  return store.runAndReturn(() => {
    setCorrelationId(id);
    return fn();
  });
}

function setCorrelationId(id) {
  store.set(CORRELATION_ID_KEY, id || shortId.generate());
  return;
}

function getCorrelationId() {
  return store.get(CORRELATION_ID_KEY);
}


module.exports = {
  withCorrelationId,
  setCorrelationId,
  getCorrelationId,
  bindEmitter: store.bindEmitter.bind(store),
  bind: store.bind.bind(store),
};