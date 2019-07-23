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
  publishNumber(0);
}


function publishNumber(myNumber) {

  logger.debug(`Publishing number ${myNumber}`);
  event.publishExpectingResponse(eventName, {number: myNumber})
  .then((response) => {
    logger.debug(`Got response of: ${response.number}`);
  })
  .catch((err) => {
    logger.debug('Error occured during event stream request.');
    logger.debug(err);
  });

}


//-------------------------------------------------
// Subscribe
//-------------------------------------------------
function startSubscribing() {

  event.subscribe(eventName, async (message) => {

    logger.debug(`New ${eventName} event message:`, message);

    // Let's pretend this involved an async operation, e.g. database read.
    await Promise.delay(1000)
    .then(() => {
      return;
    });

    class InvalidNumberForDoubling extends Error {
      constructor(message = 'Can not double that number') {
        super(message);
        // Ensure the name of this error is the same as the class name.
        this.name = this.constructor.name;
        // Capturing stack trace, excluding constructor call from it.
        Error.captureStackTrace(this, this.constructor);
        // Add a statusCode, useful when converting an error object to a HTTP response
        this.statusCode = 400;
      }
    }

    if (message.number > 0) {
      return {number: message.number * 2};
    } else {
      return new InvalidNumberForDoubling(`Unable to double: ${message.number}`);
    }
    
    
  })
  .then(() => {
    logger.debug(`Subscribed to ${eventName} events`);
  })
  .catch((err) => {
    logger.error(`Failed to subscribe to ${eventName} event`, err);
  });

}