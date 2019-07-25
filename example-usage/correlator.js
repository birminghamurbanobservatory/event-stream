const cls = require(`cls-hooked`);
const shortId = require(`shortid`);

const store = cls.createNamespace(`correlation-id-namespace`);

const CORRELATION_ID_KEY = `correlation-id`;

// executes specified function with correlation ID. If ID is missing then new ID is generated
async function withId(fn, id) {
  return store.runAndReturn(() => {
    setId(id);
    return fn();
  });
}

function setId(id) {
  store.set(CORRELATION_ID_KEY, id || shortId.generate());
  return;
}

function getId() {
  return store.get(CORRELATION_ID_KEY);
}

module.exports = {
  withId,
  getId,
  setId,
  bindEmitter: store.bindEmitter.bind(store),
  bind: store.bind.bind(store),
};