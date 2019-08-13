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
  startSubscribing();
  startPublishing();
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



let myNumber = 1;


//-------------------------------------------------
// Publish
//-------------------------------------------------
function startPublishing() {

  setInterval(() => {
    publishHello();
  }, 3000);

}


function publishHello() {

  logger.debug(`Publishing now`);
  event.publishExpectingResponse(eventName, 'Hello there')
  .then((response) => {
    logger.debug(`Got response: ${response}`); // this should be undefined
  })
  .catch((err) => {
    logger.debug('Failed to publish', err);
  });

}


//-------------------------------------------------
// Subscribe
//-------------------------------------------------
function startSubscribing() {

  event.subscribe(eventName, async (message) => {

    logger.debug(`New ${eventName} event message:`, message);

    // Let's pretend this involved an async operation, e.g. database read.
    return await Promise.delay(500)
    .then(() => {
      return; // i.e. return nothing
    });
    
  })
  .then(() => {
    logger.debug(`Subscribed to ${eventName} events`);
  })
  .catch((err) => {
    logger.error(`Failed to subscribe to ${eventName} event`, err);
  });

}