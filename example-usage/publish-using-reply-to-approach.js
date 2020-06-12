//-------------------------------------------------
// Dependencies
//-------------------------------------------------
const event = require('../index');
const logger = require('node-logger');
const Promise = require('bluebird');
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


//-------------------------------------------------
// Publish
//-------------------------------------------------
function startPublishing() {

  setInterval(() => {
    publishNumber();
  }, 3000);

}


function publishNumber() {

  const number = new Date().getSeconds();

  // Note: this approach is fundamentally different to using publishExpectingResponse
  event.publish('stage-1', {number}, {replyTo: 'stage-2'})
  .then(() => {
    logger.debug(`Published the number ${number} ok`);
  })
  .catch((err) => {
    logger.debug('Failed to publish', err);
  });

}


//-------------------------------------------------
// Subscribe
//-------------------------------------------------
function startSubscribing() {

  // There's actually two sets of events we need to subscribe to here. Stage 1 and stage 2.

  // Stage 1
  event.subscribe('stage-1', (message) => {
    logger.debug(`Stage 1 subscriber has received the number ${message.number} ok`)
    // Add an artificial delay
    return Promise.delay(500)
    .then(() => {
      return {number: message.number * 100}; 
    });
  })
  .then(() => {
    logger.debug(`Subscribed to 'stage-1' events`);
  })
  .catch((err) => {
    logger.error(`Failed to subscribe to 'stage-1' event`, err);
  });


  // Stage 2
  event.subscribe('stage-2', (message) => {
    logger.debug(`Stage 2 subscriber has received the number ${message.number} ok`);
  })
  .then(() => {
    logger.debug(`Subscribed to 'stage-2' events`);
  })
  .catch((err) => {
    logger.error(`Failed to subscribe to 'stage-2' event`, err);
  });

}