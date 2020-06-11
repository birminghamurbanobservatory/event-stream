// Correlation ids are used to track a request that may reply on multiple microservices to fufill. They differ from the replyId used by the event-stream, this is purely used to identify the response to a request.

// IMPORTANT: Make sure the equivalent subscriber file is running first, e.g. in a separate tab.
// Made sense to run the publisher and subscriber separately to ensure the correlationId had come through the event-stream rather than some local "short-circuit".

//-------------------------------------------------
// Dependencies
//-------------------------------------------------
const event = require('../index');
const logger = require('node-logger');
const correlator = require('./correlator');

//-------------------------------------------------
// Config
//-------------------------------------------------
logger.configure({
  level: 'debug', 
  enabled: true, 
  format: 'terminal',
  getCorrelationId: correlator.getCorrelationId
});
const url = 'amqp://localhost';
const appName = 'example-app';
const eventName = 'double-my-number';


//-------------------------------------------------
// Init
//-------------------------------------------------
event.init({
  url, 
  appName,
  withCorrelationId: correlator.withCorrelationId,
  getCorrelationId: correlator.getCorrelationId
})
.then(() => {
  logger.debug('Initialisation ok');
  startPublishing();
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
// Publish
//-------------------------------------------------
function startPublishing() {

  setTimeout(() => {
    publishNumber(13);
  }, 1000);

}


function publishNumber(myNumber) {

  const correlationId = `aaaaaa${myNumber}`;

  correlator.withCorrelationId(() => {

    logger.debug(`Publishing number ${myNumber}`);
    event.publishExpectingResponse(eventName, {number: myNumber})
    .then((response) => {
      logger.debug(`Got response of: ${response.number}`);
    })
    .catch((err) => {
      logger.debug('Got an error back from the subscriber', err);
    });

  }, correlationId);

}



function startSubscribing() {

  event.subscribe(eventName, async (message) => {

    logger.debug(`New ${eventName} event message:`, message);

    // Let's see if the correlationId is available.
    const correlationId = correlator.getCorrelationId();
    logger.debug(`CorrelationId: ${correlationId}`);
    
    throw new Error('Example bad request');

    // Let's pretend this involved an async operation, e.g. database read.
    const doubled = await Promise.delay(1000)
    .then(() => {
      return message.number * 2;
    });

    return {number: doubled};
    
  })
  .then(() => {
    logger.debug(`Subscribed to ${eventName} events`);
  })
  .catch((err) => {
    logger.error(`Failed to subscribe to ${eventName} event`, err);
  });

}