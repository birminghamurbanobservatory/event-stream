//-------------------------------------------------
// Dependencies
//-------------------------------------------------
const event = require('../index');
const logger = require('node-logger');
const Promise = require('bluebird');

//-------------------------------------------------
// Config
//-------------------------------------------------
logger.configure({level: 'debug', enabled: true, format: 'terminal'});
const url = 'amqp://localhost';
const appName = 'example-app';
const eventName = 'some-event';


//-------------------------------------------------
// Init
//-------------------------------------------------
event.init({url, appName})
.then(() => {
  logger.debug('Initialisation ok');
  startPublishing();
})
.catch((err) => {
  logger.error('Error during event-stream initialisation', err);
});

event.logsEmitter.on('error', (msg) => {
  logger.error(msg);
});
event.logsEmitter.on('warn', (msg) => {
  logger.warn(msg);
});
event.logsEmitter.on('info', (msg) => {
  logger.info(msg);
});
event.logsEmitter.on('debug', (msg) => {
  logger.debug(msg);
});



//-------------------------------------------------
// Publish
//-------------------------------------------------
function startPublishing() {
  publishNumber(22);
}


function publishNumber(myNumber) {

  logger.debug(`Publishing number ${myNumber}`);
  event.publishExpectingResponse(eventName, {number: myNumber}, {timeout: 500})
  .then((response) => {
    logger.debug(`Got response of: ${response.number}`);
  })
  .catch((err) => {
    logger.debug('Failed during publish expecting response', err);
  });

}

