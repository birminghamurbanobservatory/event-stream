//-------------------------------------------------
// Dependencies
//-------------------------------------------------
const amqp   = require('amqplib');
const joi    = require('@hapi/joi');
const check  = require('check-types');
const shortId = require('shortid');
const Promise = require('bluebird');
const util         = require('util');
const EventEmitter = require('events').EventEmitter;


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
// Create an event emitter
//-------------------------------------------------
function LogsObservable() {  
  EventEmitter.call(this);
}
util.inherits(LogsObservable, EventEmitter); 

LogsObservable.prototype.error = function (message) {  
  this.emit('error', message);
};
LogsObservable.prototype.warn = function (message) {  
  this.emit('warn', message);
};
LogsObservable.prototype.info = function (message) {  
  this.emit('info', message);
};
LogsObservable.prototype.debug = function (message) {  
  this.emit('debug', message);
};

const logsEmitter = new LogsObservable();

//-------------------------------------------------
// Module Exports
//-------------------------------------------------
exports = module.exports = {
  init,
  publish,
  publishExpectingResponse,
  subscribe,
  logsEmitter,
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
      .required(),
    withCorrelationId: joi.func(),
    getCorrelationId: joi.func()
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
    logsEmitter.info('Event stream connection has been made');
    return;
  })
  .catch((err) => {
    logsEmitter.error(`Failed to connect to event stream. Reason: ${err.message}`);
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
      logsEmitter.error(`Event stream connection error: ${err.message}`);
    });

    _conn.on('close', (err) => {
      logsEmitter.info(`Event stream connection closed: ${err.message}`);
      reactToFailedConnection();
    });

    return _conn.createChannel()
    .then((channel) => {
      _channel = channel;
      _connected = true;

      _channel.on('error', (err) => {
        logsEmitter.error(`AMQP channel error: ${err.message}`);
      });

      _channel.on('close', () => {
        // A channel close event provides no err object unlike a connection close event.
        logsEmitter.info('The AMQP channel was closed');
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
    return Promise.reject(new NoEventStreamConnection('Currently not connected to the event stream'));
  }

  // Validate the opts object
  const schema = joi.object({
    correlationId: joi.string()
      .min(5)
  });

  const {error: err, value: options} = joi.validate(opts, schema);

  if (err) {
    Promise.reject(new EventStreamError(`Invalid publish options: ${err.message}`));
  }  

  let correlationId;
  // Use the correlationId passed to the function by default
  if (options.correlationId) {
    correlationId = options.correlationId;
  // If not see if one is available via the getCorrelationId function. If not generate one.
  } else if (check.assigned(_options.getCorrelationId)) {
    correlationId = _options.getCorrelationId() || shortId.generate();
  } else {
    correlationId = shortId.generate();
  }

  const message = {
    body,
    correlationId
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
    return Promise.reject(new NoEventStreamConnection('Currently not connected to the event stream'));
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

  let correlationIdIfNonePassedIn;
  if (check.assigned(_options.getCorrelationId)) {
    correlationIdIfNonePassedIn = _options.getCorrelationId() || shortId.generate();
  } else {
    correlationIdIfNonePassedIn = shortId.generate();
  }

  const defaultOptions = {
    correlationId: correlationIdIfNonePassedIn,
    replyTo: shortId.generate(),
    timeout: 5000
  }; 

  const replyId = shortId.generate();

  const options = Object.assign({}, defaultOptions, validOptions);

  const message = {
    body,
    correlationId: options.correlationId,
    replyTo: options.replyTo,
    replyId
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

          if (msg.replyId === replyId) {
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
    logsEmitter.debug(`The replyTo queue (${queueName}) has been deleted.`);
  })
  .catch((err) => {
    logsEmitter.error(`Failed to delete queue: ${queueName}. Reason: ${err.message}`);
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

    const expectingReply = check.nonEmptyObject(message) && check.nonEmptyString(message.replyTo) && check.nonEmptyString(message.replyId);

    // Set the current correlationId, i.e. so loggers in the client's cbFunc can access it.
    const bindCorrelationIdToCbFunc = check.nonEmptyObject(message) && check.nonEmptyString(message.correlationId) && check.assigned(_options.withCorrelationId);

    if (expectingReply) {
      logsEmitter.debug(`A ${eventName} event has been received with correlationId: ${message.correlationId}, which expects a response on a replyTo queue named: ${message.replyTo} with a replyId of ${message.replyId}`);
      let response;
      try {
        if (bindCorrelationIdToCbFunc) {
          response = await _options.withCorrelationId(async () => {
            const resultOfCbFunc = await cbFunc(message.body);
            return resultOfCbFunc;
          }, message.correlationId);
        } else {
          response = await cbFunc(message.body);
        }
      } catch (err) {
        response = err;
      }

      try {
        await respondToRequest(response, message.replyTo, message.replyId, message.correlationId);
      } catch (err) {
        logsEmitter.error(`Failed to respond to an request that expected a response. Reason: ${err.message}`);
      }

    } else {
      if (bindCorrelationIdToCbFunc) {
        await _options.withCorrelationId(async () => {
          await cbFunc(message.body);
          return;
        }, message.correlationId);
      } else {
        await cbFunc(message.body);
      } 
    }

    return;
  };

  // Add this subscription to our list so we can add them again if connection ever goes down.
  _subscriptions.push({eventName, cbFunc: cbFuncWithWrapper});

  if (_connected !== true) {
    return Promise.reject(new EventStreamError('Currently not connected to the event stream'));
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
function respondToRequest(response, replyTo, replyId, correlationId) {

  if (!(check.string(response) || check.object(response) || check.array(response) || check.instance(response, Error))) {
    return Promise.reject(new EventStreamError('The response must be a string, POJO or Error object.'));
  }

  if (check.not.nonEmptyString(replyTo)) {
    return Promise.reject(new EventStreamError('replyTo should be a non-empty string'));
  }  

  if (check.not.nonEmptyString(replyId)) {
    return Promise.reject(new EventStreamError('replyId should be a non-empty string'));
  }

  const errorOccurred = check.instance(response, Error);

  const messageToSend = {
    replyId
  };

  if (correlationId) {
    messageToSend.correlationId = correlationId;
  }
  
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

  logsEmitter.debug(`Responding to request on queue: ${replyTo}, with replyId: ${replyId}, correlationId: ${correlationId}, and response: ${response}`);

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
    logsEmitter.debug('Already trying to re-establish the connection');
  } else {
    logsEmitter.info('Retrying event stream connection straight away');
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
    logsEmitter.info('Event stream connection reconnected successfully');
    _retrying = false;
    _currentWaitTime = _waitTimes[0];

    if (_subscriptions.length > 0) {

      // Re-establish any subscriptions we had
      return Promise.map(_subscriptions, (sub) => {
        logsEmitter.debug(`About to try reconnecting ${sub.eventName}`);
        return consume(sub.eventName, sub.cbFunc)
        .then(() => {
          logsEmitter.info(`Successfully re-established the ${sub.eventName} subscription`);
          return;
        })
        .catch((err) => {
          logsEmitter.error(`Failed to re-establish the ${sub.eventName} subscription. Reason: ${err.message}`);
          return;
        });
      });

    } else {
      logsEmitter.info('There were no subscriptions that needed re-establishing after the reconnect');
    }

  })
  .catch((err) => {

    logsEmitter.error(`Reconnect failed. Reason: ${err.message}`);

    logsEmitter.info(`Will try to reconnect event stream connection in ${_currentWaitTime} seconds`);
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

