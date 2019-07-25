//-------------------------------------------------
// Dependencies
//-------------------------------------------------
const amqp   = require('amqplib');
const joi    = require('@hapi/joi');
const logger = require('node-logger');
const check  = require('check-types');
const shortid = require('shortid');
const Promise = require('bluebird');


//-------------------------------------------------
// Load in some custom errors
//-------------------------------------------------
const {
  EventStreamError,
  InvalidPublishBody,
  InvalidEventName,
  EventStreamResponseError,
  EventStreamOperationalError,
  NoEventStreamConnection,
  EventStreamResponseTimeout
} = require('./errors');


//-------------------------------------------------
// Module Exports
//-------------------------------------------------
exports = module.exports = {
  init,
  publish,
  publishExpectingResponse,
  subscribe,
  EventStreamError, // handy to export these errors so client code can identify them (i.e. with 'instanceof')
  EventStreamOperationalError,
  EventStreamResponseError
};

// Module globals
let _initialised = false;
let _conn;
let _channel;
const _waitTimes = [5, 10, 30, 60];
let _currentWaitTime = _waitTimes[0];
let _retrying = false;
let _connected = false;
let _options;
const _subscriptions = [];


//-------------------------------------------------
// Init
//-------------------------------------------------
// returns a promise when it's ready to accept publish and subscribe's.
function init(opts) {

  if (_initialised === true) {
    return Promise.reject(new EventStreamError('Event stream already initialised'));
  }

  // Validate the opts object
  const schema = joi.object({
    url: joi.string()
            .uri()
            .required(),
    appName: joi.string()
      .required()
  })
  .required();

  const {error: err, value: options} = joi.validate(opts, schema);

  if (err) {
    return Promise.reject(new EventStreamError(`Invalid init options: ${err.message}`));
  }

  _options = options;

  _initialised = true; // so we know it shouldn't be initialised again

  // Set up the connection - connections are expensive to open hence why we try to only have one open.
  return connect(_options.url)
  .then(() => {
    logger.info('Event stream connection has been made');
    return;
  })
  .catch((err) => {
    logger.error('Failed to connect to event stream', err);
    reactToFailedConnection();
    return Promise.reject(new NoEventStreamConnection('Failed to initialise event stream connection'));
  });

}


//-------------------------------------------------
// connect
//-------------------------------------------------
function connect(url) {
  const open = amqp.connect(url);
  return open.then((conn) => {
    _conn = conn;

    _conn.on('error', (err) => {
      logger.error('Event stream connection error', err);
    });

    _conn.on('close', (err) => {
      logger.warn('The event stream connection was closed', err);
      reactToFailedConnection();
    });

    return _conn.createChannel()
    .then((channel) => {
      _channel = channel;
      _connected = true;

      _channel.on('error', (err) => {
        logger.error('AMQP channel error', err);
      });

      _channel.on('close', () => {
        // A channel close event provides no err object unlike a connection close event.
        logger.warn('The AMQP channel was closed');
        reactToFailedConnection();
      });

      return;
    });

  });
}


//-------------------------------------------------
// Publish
//-------------------------------------------------
// For when we want to add a new message to the RabbitMQ queue
// Returns a promise
function publish(eventName, body, opts = {}) {

  if (check.not.nonEmptyString(eventName)) {
    return Promise.reject(new InvalidEventName('eventName should be a non-empty string'));
  }

  if (!(check.string(body) || check.object(body) || check.array(body))) {
    return Promise.reject(new InvalidPublishBody('The message body must be a string or POJO'));
  }

  if (_initialised !== true) {
    return Promise.reject(new EventStreamError('Event stream must first be initialised'));
  }

  if (_connected !== true) {
    return Promise.reject(new NoEventStreamConnection('Must establish event stream connection first'));
  }

  // Validate the opts object
  const schema = joi.object({
    correlationId: joi.string()
      .min(5)
  });

  const {error: err, value: validOptions} = joi.validate(opts, schema);

  if (err) {
    Promise.reject(new EventStreamError(`Invalid publish options: ${err.message}`));
  }  

  const defaultOptions = {
    correlationId: shortid.generate()
  };   

  const options = Object.assign({}, defaultOptions, validOptions);

  const message = {
    body,
    correlationId: options.correlationId
  };

  // Convert message to a buffer
  const messageBuffer = convertToBuffer(message);

  // Create the exchange, if it doesn't already exist. Setting it as durable means that if amqp quits/crashes then the queue won't be lost.
  return _channel.assertExchange(eventName, 'fanout', {durable: true})
  .then(() => {
    // Marking the messages as persistent will mean RabbitMQ will save them to disk, thus reducing the chances of messages being lost if RabbitMQ restarts.
    const publishOptions = {persistent: true};
    return _channel.publish(eventName, '', messageBuffer, publishOptions);
  })
  .catch((err) => {

    // If the error is because the channel is closed, then let's try to reconnect
    if (err.message === 'Channel closed') {
      reactToFailedConnection();
    }

    // Continue to pass on the error so if can be handled further down the chain
    return Promise.reject(err);
  });

}


//-------------------------------------------------
// Publish a request and wait for a response
//-------------------------------------------------
// This allows for the request/response pattern.
// Further info: https://medium.com/@pulkitswarup/microservices-asynchronous-request-response-pattern-6d00ab78abb6
function publishExpectingResponse(eventName, body, opts) {

  if (check.not.nonEmptyString(eventName)) {
    return Promise.reject(new InvalidEventName('eventName should be a non-empty string'));
  }

  if (!(check.string(body) || check.object(body) || check.array(body))) {
    return Promise.reject(new InvalidPublishBody('The message body must be a string or POJO'));
  }    

  if (_initialised !== true) {
    return Promise.reject(new EventStreamError('Event stream must first be initialised'));
  } 

  if (_connected !== true) {
    return Promise.reject(new NoEventStreamConnection('Must establish event stream connection first'));
  }

  const optionsSchema = joi.object({
    correlationId: joi.string()
      .min(5),
    replyTo: joi.string(),
    timeout: joi.number()
      .min(1)
      .max(30000)
  });

  const {error: err, value: validOptions} = joi.validate(opts, optionsSchema);

  if (err) {
    return Promise.reject(new EventStreamError(`Invalid opts: ${err.message}`));
  }

  const defaultOptions = {
    correlationId: shortid.generate(),
    replyTo: shortid.generate(),
    timeout: 5000
  }; 

  const options = Object.assign({}, defaultOptions, validOptions);

  const message = {
    body,
    correlationId: options.correlationId,
    replyTo: options.replyTo
  };

  const messageBuffer = convertToBuffer(message);

  let gotResponse = false;

  // Begin by listening to the replyTo queue
  return new Promise((resolve, reject) => {

    _channel.assertQueue(options.replyTo, {exclusive: true, autoDelete: true})
    .then(() => {

      return _channel.consume(options.replyTo, (msgBuffer) => {

        // When the queue is deleted a null message is emitted, we want to ignore this.
        if (msgBuffer !== null) {
          let msg;
          try {
            msg = convertFromBuffer(msgBuffer);
          } catch (err) {
            reject(err);
          }

          if (msg.correlationId === options.correlationId) {
            if (msg.error) {
              // N.B. this error uses the name from the event stream as its name property, rather than the name of the custom class like other custom classes would.
              reject(new EventStreamResponseError(msg.error.name, msg.error.message, msg.error.statusCode));
            } else {
              resolve(msg.body);
            }
            gotResponse = true;
            deleteReplyToQueue(options.replyTo);
          }
        }

      });

    })
    .then(() => {

      // Now to send the request
      return _channel.assertExchange(eventName, 'fanout', {durable: true})
      .then(() => {
        // Marking the messages as persistent will mean RabbitMQ will save them to disk, thus reducing the chances of messages being lost if RabbitMQ restarts.
        const publishOptions = {persistent: true};
        publishOptions.expiration = options.timeout;
        return _channel.publish(eventName, '', messageBuffer, publishOptions);
      })
      .catch((err) => {
        // If the error is because the channel is closed, then let's try to reconnect
        if (err.message === 'Channel closed') {
          reactToFailedConnection();
        }
        // Continue to pass on the error so if can be handled further down the chain
        return Promise.reject(err);
      });      

    })
    .then(() => {

      // Timeout if it takes too long to get a response
      setTimeout(() => {
        if (!gotResponse) {
          reject(new EventStreamResponseTimeout(`Timed out (${options.timeout} ms) whilst waiting for response to ${eventName}`));
          deleteReplyToQueue(options.replyTo);
        }
      }, options.timeout);

    })
    .catch((err) => {
      reject(err);
    });

  });
  
}


//-------------------------------------------------
// Delete replyTo queue
//-------------------------------------------------
function deleteReplyToQueue(queueName) {

  _channel.deleteQueue(queueName)
  .then(() => {
    logger.debug(`The replyTo queue (${queueName}) has been deleted.`);
  })
  .catch((err) => {
    logger.error(`Failed to delete queue: ${queueName}. Reason: ${err.message}`);
  });

}


//-------------------------------------------------
// Subscribe
//-------------------------------------------------
// eventName is the name of the event that will be used as the RabbitMQ queue name.
// cbFunc is the function called whenever the event occurs.
function subscribe(eventName, cbFunc) {

  if (check.not.nonEmptyString(eventName)) {
    return Promise.reject(new InvalidEventName('eventName should be a non-empty string'));
  }

  if (check.not.function(cbFunc)) {
    return Promise.reject(new EventStreamError('cbFunc should be a function'));
  }

  if (_initialised !== true) {
    return Promise.reject(new EventStreamError('Event stream must first be initialised'));
  } 

  // It's possible that the message received has a replyTo property, in this instance the original publisher (i.e. another microservice) is expecting a response, this wrapper will handle this logic so that the application using this package doesn't have to.
  const cbFuncWithWrapper = async function (message) {

    // TODO: should I check the message is valid here, before handing it over to the cbFunc?

    const expectingReply = check.nonEmptyObject(message) && check.nonEmptyString(message.replyTo);

    if (expectingReply) {
      logger.debug(`A ${eventName} has been received with correlationId: ${message.correlationId}, which expects a response on a replyTo queue named: ${message.replyTo}`);
      let response;
      try {
        // Gives the full message to the cbFunc, as the cbFunc should have access to the correlationId so it can add it to any logs to aid debugging.
        response = await cbFunc(message); 
      } catch (err) {
        response = err;
      }

      try {
        await respondToRequest(response, message.replyTo, message.correlationId);
      } catch (err) {
        logger.error('Failed to respond to an request that expected a response.', err);
      }

    } else {
      await cbFunc(message);
    }

    return;
  };

  // Add this subscription to our list so we can add them again if connection ever goes down.
  _subscriptions.push({eventName, cbFuncWithWrapper});

  if (_connected !== true) {
    return Promise.reject(new EventStreamError('Must establish event stream connection first'));
  } else {
    return consume(eventName, cbFuncWithWrapper);
  }  

}


//-------------------------------------------------
// Consume
//-------------------------------------------------
function consume(exchangeName, cbFunc) {

  if (check.not.nonEmptyString(exchangeName)) {
    return Promise.reject(new EventStreamError('exchangeName should be a non-empty string'));
  }

  if (check.not.function(cbFunc)) {
    return Promise.reject(new EventStreamError('cbFunc should be a function'));
  }


  // Create the exchange if it doesn't already exist
  return _channel.assertExchange(exchangeName, 'fanout', {durable: true})
  .then(() => {
    
    // Set the queue name as the name of the exchange postfixed by the name of the app (avoids issues with multiple instances).
    const queueName = `${exchangeName}.for-${_options.appName}`;

    return _channel.assertQueue(queueName, {exclusive: false, durable: true})
    .then((q) => {

      // Now let's bind this queue to the exchange
      return _channel.bindQueue(q.queue, exchangeName, '')
      .then(() => {
        
        // Tell the server to deliver us any messages in the queue.
        return _channel.consume(q.queue, (msg) => {
          // After converting the buffer pass it into the provided function
          cbFunc(convertFromBuffer(msg));
        }, {noAck: true});

      });
    });
  })
  .catch((err) => {

    // If the error is because the connection is closed, then let's try to reconnect
    if (err.message.startsWith('Connection closed')) {
      reactToFailedConnection();
    }

    // Continue to pass on the error so if can be handled further down the chain
    return Promise.reject(err);
  });  

}


//-------------------------------------------------
// Response to request
//-------------------------------------------------
// reponse can be a string or POJO reponse that will for the message body, or it can be an Error object in which case the message will include an error object.
function respondToRequest(response, replyTo, correlationId) {

  if (!(check.string(response) || check.object(response) || check.array(response) || check.instance(response, Error))) {
    return Promise.reject(new EventStreamError('The response must be a string, POJO or Error object.'));
  }

  if (check.not.nonEmptyString(replyTo)) {
    return Promise.reject(new EventStreamError('replyTo should be a non-empty string'));
  }  

  if (check.not.nonEmptyString(correlationId)) {
    return Promise.reject(new EventStreamError('correlationId should be a non-empty string'));
  }

  const errorOccurred = check.instance(response, Error);

  const messageToSend = {
    correlationId
  };
  
  if (errorOccurred) {
    messageToSend.error = {
      name: response.name,
      message: response.message
    };
    if (check.number(response.statusCode)) {
      messageToSend.error.statusCode = response.statusCode;
    }
  } else {
    messageToSend.body = response;
  }

  const bufferToSend = convertToBuffer(messageToSend);

  _channel.sendToQueue(replyTo, bufferToSend); // synchronous

  return Promise.resolve();

}


//-------------------------------------------------
// Convert to buffer
//-------------------------------------------------
// Converts the javascript variable to buffer than
function convertToBuffer(toSend) {

  let toSendStr;

  if (check.string(toSend)) {
    toSendStr = toSend;
  } else if (check.object(toSend) || check.array(toSend)) {
    toSendStr = JSON.stringify(toSend);
  } else {
    throw new EventStreamError(`Can only convert strings or POJO to a buffer, not: ${typeof toSend}.`);
  }

  return Buffer.from(toSendStr);

}


//-------------------------------------------------
// Convert from buffer
//-------------------------------------------------
function convertFromBuffer(buf) {

  // First get it back as a string
  const msgStr = buf.content.toString();

  // If the string is in JSON format then let's convert it to a POJO
  return isJsonString(msgStr) ? JSON.parse(msgStr) : msgStr;

}


//-------------------------------------------------
// Is string valid JSON
//-------------------------------------------------
function isJsonString(str) {
  try {
    JSON.parse(str);
  } catch (e) {
    return false;
  }
  return true;
}


//-------------------------------------------------
// React to Failed Connection
//-------------------------------------------------
function reactToFailedConnection() {

  _connected = false;

  // Are we already retrying the connection?
  if (_retrying) {
    logger.debug('Already trying to re-establish the connection');
  } else {
    logger.warn('Retrying event stream connection straight away');
    reconnect();
  }

}


//-------------------------------------------------
// Reconnect
//------------------------------------------------- 
// recursive function that keeps calling itself at longer and longer intervals if it keeps failing
function reconnect() {

  _retrying = true;

  return connect(_options.url)
  .then(() => {
    logger.info('Event stream connection reconnected successfully');
    _retrying = false;
    _currentWaitTime = _waitTimes[0];

    if (_subscriptions.length > 0) {

      // Re-establish any subscriptions we had
      return Promise.map(_subscriptions, (sub) => {
        logger.debug(`About to try reconnecting ${sub.eventName}`);
        return consume(sub.eventName, sub.cbFunc)
        .then(() => {
          logger.info(`Successfully re-established the ${sub.eventName} subscription`);
          return;
        })
        .catch((err) => {
          logger.error(`Failed to re-establish the ${sub.eventName} subscription`);
          return;
        });
      });

    } else {
      logger.info('There were no subscriptions that needed re-establishing after the reconnect');
    }

  })
  .catch((err) => {

    logger.error('Reconnect failed.', err);

    logger.warn(`Will try to reconnect event stream connection in ${_currentWaitTime} seconds`);
    // Call itself again, the 'bind' here is crucial
    setTimeout(reconnect, _currentWaitTime * 1000);
    updateCurrentWaitTime(); // will make the waitTime longer for next time

  });

}


//-------------------------------------------------
// Update waitTime
//-------------------------------------------------
function updateCurrentWaitTime() {
  
  // Wait time won't ever be any longer than the final value in the waitTimes array.
  const idx = _waitTimes.indexOf(_currentWaitTime);

  if (idx >= 0 && idx < _waitTimes.length - 1)
    _currentWaitTime = _waitTimes[idx + 1];
  else if (idx === _waitTimes.length - 1) {
    // Do nothing, i.e. keep it at this maximum wait time
  } else {
    _currentWaitTime = _waitTimes[0];
  }

}

