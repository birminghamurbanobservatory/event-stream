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

let myNumber = 1;


//-------------------------------------------------
// Publish
//-------------------------------------------------
function startPublishing() {

  setInterval(() => {
    publishNumber(myNumber);
    myNumber++;
  }, 4000);

}


function publishNumber(myNumber) {

  logger.debug(`Publishing number ${myNumber}`);
  event.publishExpectingResponse(eventName, {number: myNumber})
  .then((response) => {
    logger.debug(`Got response of: ${response.number}`);
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