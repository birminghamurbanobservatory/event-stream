//-------------------------------------------------
// Dependencies
//-------------------------------------------------
const event = require('../../index');
const logger = require('node-logger');
const Promise = require('bluebird');

//-------------------------------------------------
// Config
//-------------------------------------------------
logger.configure({level: 'debug', enabled: true, format: 'terminal'});
const url = 'amqp://localhost';
const appName = 'example-app';
const eventName = 'queue-with-spikes';
const nMessages = 100;
const numbers = Array.from({length: nMessages}, (_, i) => i + 1);


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

  // Publish a load a messages in one go
  Promise.each(numbers, (number) => {
    return event.publish(eventName, number.toString());
  })
  .then(() => {
    logger.debug('A load of messages have been published');
  });

}



