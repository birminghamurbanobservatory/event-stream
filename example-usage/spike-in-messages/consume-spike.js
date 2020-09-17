//-------------------------------------------------
// Dependencies
//-------------------------------------------------
const event = require('../../index');
const logger = require('node-logger');
const Promise = require('bluebird');
const _ = require('lodash');

// IMPORTANT: You'll need to run the equivalent publisher file once this is running first, e.g. in a separate tab.
// Made sense to run the publisher and subscriber separately to ensure the correlationId had come through the event-stream rather than some local "short-circuit".

//-------------------------------------------------
// Config
//-------------------------------------------------
logger.configure({level: 'debug', enabled: true, format: 'terminal'});
const url = 'amqp://localhost';
const appName = 'example-app';
const eventName = 'queue-with-spikes';
const maxMessagesAtOnce = 5;
const minProcessTime = 2000
const maxProcessTime = 3000;


//-------------------------------------------------
// Init
//-------------------------------------------------
event.init({
  url, 
  appName,
  maxMessagesAtOnce
})
.then(() => {
  logger.debug('Initialisation ok');
  startSubscribing();
})
.catch((err) => {
  logger.error('Error during event-stream initialisation', err);
  // Let's add the subscriptions even if the init failed (e.g. because RabbitMQ wasn't turned on yet), this ensures the subscriptions get added to the list and will be automatically re-established if the connection returns.  
  startSubscribing();
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
// Subscribe
//-------------------------------------------------
function startSubscribing() {

  event.subscribe(eventName, async (message) => {

    logger.debug(`New ${eventName} event message:`, message);

    // Let's pretend this involved an async operation, e.g. database read.
    await Promise.delay(_.random(minProcessTime, maxProcessTime));

    logger.debug(`Finished processing message ${message}`);

    return;
    
  })
  .then(() => {
    logger.debug(`Subscribed to ${eventName} events`);
  })
  .catch((err) => {
    logger.error(`Failed to subscribe to ${eventName} event`, err);
  });

}

