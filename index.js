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
  EventStreamResponseTimeout,
  FailedToJoinDirectReplyToQueue,
  ConnectDuringReconnectFail
} = require('./errors');


//-------------------------------------------------
// Create an event emitter for logs
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
// Create an event emitter for replyTo responses
//-------------------------------------------------
const replyEmitter = new EventEmitter();


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
const _configForRequestQueues = {durable: false, exclusive: false, 'x-queue-mode': 'default'};
const _configForFireForgetQueues = {durable: true, exclusive: false, 'x-queue-mode': 'lazy'};
const _typeForErrors = 'error';
const _noContentString = '---no-content---';

// -- Request queues config --
// The messages on these queues will have a time limit, and need to be handled in a timely fashion because a microservice (and probably a human) is actively waiting for a response.
// Therefore there they should not be lazy in order to incrase throughput speed, and don't need to be durable and the messages themselves won't be persistant.

// -- Queues for fire/forget messages --
// These type of queues will be used when the message can be handled in a less timely fashion, e.g. processing of observations. We also want to ensure the messages on these queues won't be lost, hence making it durable.
// durable: true, means the queues are recoved if RabbitMQ restarts
// exclusive: false, will allow multiple instances of a microservices to listen to the same queue.
// 'x-queue-mode': 'lazy', provides more predictive performance as the messages will be automatically stored to disk (https://www.cloudamqp.com/blog/2017-12-29-part1-rabbitmq-best-practice.html).



//-------------------------------------------------
// Init
//-------------------------------------------------
// returns a promise when it's ready to accept publish and subscribe's.
function init(opts) {

  logsEmitter.debug('Starting initialisation of event stream');

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

  
  const {error: err, value: options} = schema.validate(opts);
  
  if (err) {
    return Promise.reject(new EventStreamError(`Invalid init options: ${err.message}`));
  }

  logsEmitter.debug(`Initialising event stream with url: ${options.url}, and appName: ${options.appName}.`);

  _options = options;

  _initialised = true; // so we know it shouldn't be initialised again

  // Set up the connection - connections are expensive to open hence why we try to only have one open.
  return connect(_options.url)
  .then(() => {
    logsEmitter.info('Event stream connection has been made');
  })
  .catch((err) => {
    logsEmitter.error(`Failed to connect to event stream. Reason: ${err.message}`);
    reactToFailedConnection();
    return Promise.reject(new NoEventStreamConnection('Failed to initialise event stream connection'));
  })
  .then(() => {
    // If it was able to connect then we should start listening on the Direct Reply-To Queue
    return listenToDirectReplyToQueue();
  })
  .then(() => {
    logsEmitter.debug('Succesfully listening to the direct reply-to queue.');
    return;
  });

}


//-------------------------------------------------
// Listen for all replies
//-------------------------------------------------
// In order to support a RPC communication (i.e. request/reply) we'll use RabbitMQ's Direct Reply-to system (https://www.rabbitmq.com/direct-reply-to.html).
// This requires us to listen to a single queue to which all the replies will come through. What's clever about this particular system is that the replies will only ever be received by the particular microservice instance that made the request. 
// We use a event-emitter approach to emit when the reply comes in, to which the code that made the request can listen to.
function listenToDirectReplyToQueue() {

  // The 'amq.rabbitmq.reply-to' already exists within RabbitMQ, there's no need to declare it first.
  return _channel.consume('amq.rabbitmq.reply-to', (rawMsg) => {

    const replyCorrelationId = rawMsg.properties.correlationId;

    if (replyCorrelationId) {
      replyEmitter.emit(replyCorrelationId, rawMsg);
    }

  }, {noAck: true})
  .catch(() => {
    logsEmitter.error('Failed to join the direct reply-to queue');
    return Promise.reject(new FailedToJoinDirectReplyToQueue());
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

  const isBody = check.assigned(body); 
  if (isBody && (!(check.string(body) || check.object(body) || check.array(body)))) {
    return Promise.reject(new InvalidPublishBody('The message body must be a string or POJO'));
  }

  if (_initialised !== true) {
    return Promise.reject(new EventStreamError('Event stream must first be initialised'));
  }

  if (_connected !== true) {
    return Promise.reject(new NoEventStreamConnection('Currently not connected to the event stream'));
  }

  logsEmitter.debug(`Publishing a new message with eventName: ${eventName}`);

  // Validate the opts object
  const schema = joi.object({
    correlationId: joi.string().min(5), // this is the correlationId used for tracking an action throughout multiple microservices.
    replyTo: joi.string() // if the subscriber produces a result you can ask it to publish the result to this event name. N.b. unlike the publishExpectingReponse function below (which uses RabbitMQ's Direct Reply-To queues instead) this function does not start listening for responses with this event name.
  });

  const {error: err, value: options} = schema.validate(opts);

  if (err) {
    Promise.reject(new EventStreamError(`Invalid publish options: ${err.message}`));
  }  

  let trackingCorrelationId;
  // Use the correlationId passed to the function if available
  if (options.correlationId) {
    trackingCorrelationId = options.correlationId;
  // If not see if one is available via the getCorrelationId function. If not generate one.
  } else if (check.assigned(_options.getCorrelationId)) {
    trackingCorrelationId = _options.getCorrelationId() || shortId.generate();
  } else {
    trackingCorrelationId = shortId.generate();
  }

  const messageBuffer = convertToBuffer(body);

  return _channel.assertExchange(eventName, 'fanout', {durable: true})
  .then(() => {
    // Marking the messages as persistent will mean RabbitMQ will save them to disk, thus reducing the chances of messages being lost if RabbitMQ restarts.
    const publishOptions = {
      persistent: true,
      messageId: trackingCorrelationId
      // N.B. Do not set a property called 'correlationId' because the presense of this key would tell the subscriber that a Direct reply-to approach is being used. It's fine to use the messageId for our application's tracking correlation id though.
    };
    if (options.replyTo) {
      publishOptions.replyTo = options.replyTo;
    }
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

  const isBody = check.assigned(body); 
  if (isBody && (!(check.string(body) || check.object(body) || check.array(body)))) {
    return Promise.reject(new InvalidPublishBody('The message body must be a string or POJO'));
  }

  if (_initialised !== true) {
    return Promise.reject(new EventStreamError('Event stream must first be initialised'));
  } 

  if (_connected !== true) {
    return Promise.reject(new NoEventStreamConnection('Currently not connected to the event stream'));
  }

  logsEmitter.debug(`Publishing a new message (expecting response) with eventName: ${eventName}`);

  const optionsSchema = joi.object({
    correlationId: joi.string() // used to track an action through multiple microservice, NOT to match a reponse to a request
      .min(5),
    timeout: joi.number()
      .min(1)
      .max(30000)
  });

  const {error: err, value: validOptions} = optionsSchema.validate(opts);

  if (err) {
    return Promise.reject(new EventStreamError(`Invalid opts: ${err.message}`));
  }

  let trackingCorrelationIdIfNoneProvided;
  if (check.assigned(_options.getCorrelationId)) {
    trackingCorrelationIdIfNoneProvided = _options.getCorrelationId() || shortId.generate();
  } else {
    trackingCorrelationIdIfNoneProvided = shortId.generate();
  }

  const defaultOptions = {
    correlationId: trackingCorrelationIdIfNoneProvided,
    timeout: 5000
  };

  const options = Object.assign({}, defaultOptions, validOptions);

  const messageBuffer = convertToBuffer(body);
  
  const replyCorrelationId = shortId.generate();

  return _channel.assertExchange(eventName, 'fanout', {durable: true})
  .then(() => {
    const publishOptions = {
      persistent: false, // doesn't make sense to recover messages, e.g. if rabbitMQ restarts, because probably timed-out anyway.
      replyTo: 'amq.rabbitmq.reply-to', // this will make use of RabbitMQ's "Direct Reply-to" feature
      expiration: options.timeout, // if no reply after a while then remove the message.
      correlationId: replyCorrelationId,  // used to match a response to a request
      messageId: options.correlationId // used to track an action through multiple microservices
    };
    return _channel.publish(eventName, '', messageBuffer, publishOptions);
  })
  .then(() => {

    // Create a new promise
    return new Promise((resolve, reject) => {

      // Now we need to wait for a response
      replyEmitter.on(replyCorrelationId, (rawMsg) => {

        const content = convertFromBuffer(rawMsg);

        const trackingCorrelationIdInReply = rawMsg.properties.messageId;
        const canApplyCorrelationId = trackingCorrelationIdInReply && _options.withCorrelationId;

        const isErrorMessage = rawMsg.properties.type === _typeForErrors;
        let responseError;
        if (isErrorMessage) {
          responseError = new EventStreamResponseError(content.name, content.message, content.statusCode);
        }

        if (canApplyCorrelationId) {
          _options.withCorrelationId(() => {
            if (!isErrorMessage) {
              resolve(content);
            } else {
              reject(responseError);
            }
          }, trackingCorrelationIdInReply);
        } else {
          if (!isErrorMessage) {
            resolve(content);
          } else {
            reject(responseError);
          }
        }

        // Now that this response has been handled we can remove the listener.
        replyEmitter.removeAllListeners(replyCorrelationId);
      });

      setTimeout(() => {
        reject(new EventStreamResponseTimeout(`Timed out (${options.timeout} ms) whilst waiting for response to ${eventName}`));
        // Still need to tidy up the event emitter.
        replyEmitter.removeAllListeners(replyCorrelationId);
      }, options.timeout);

    });

  })
  .catch((err) => {
    // If the error is because the channel is closed, then let's try to reconnect
    if (err.message === 'Channel closed') {
      reactToFailedConnection();
    }
    // Continue to pass on the error so it can be handled further down the chain
    return Promise.reject(err);
  });      
  
}



//-------------------------------------------------
// Subscribe
//-------------------------------------------------
// eventName is the name of the event that will be used as the RabbitMQ queue name.
// cbFunc is the function called whenever the event occurs.
// Use the word 'request' in the eventName to optimise the queue type
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

  logsEmitter.debug(`Adding a new subscription to the eventName: ${eventName}`);

  // It's possible that the message received has a replyTo property, in this instance the original publisher (i.e. another microservice) is expecting a response, this wrapper will handle this logic so that the application using this package doesn't have to.
  const cbFuncWithWrapper = async function (message) {

    // N.b. the replyCorrelationId differs from the trackingCorrelationId. The former ensures the response can be matched to the request whilst the latter is used to trace/track an action through multiple microservices. The intended place in the message for the former is as the value of the correlationId in the properties, so this is what we'll use. We'll then use the messageId property for the trackingCorrelationId. 
    const replyToQueue = message.properties.replyTo; // could be undefined if the client isn't expecting a response.
    const replyCorrelationId = message.properties.correlationId; // could be undefined
    const expectingReply = check.nonEmptyString(replyToQueue);
    const expectingDirectReply = expectingReply && replyCorrelationId;
    const content = convertFromBuffer(message);
    const trackingCorrelationId = message.properties.messageId;
    const canBindCorrelationIdToCbFunc = check.nonEmptyString(trackingCorrelationId) && check.assigned(_options.withCorrelationId);

    //------------------------
    // Expecting a response
    //------------------------
    if (expectingReply) {
      logsEmitter.debug(`A ${eventName} event has been received with a reply correlationId of ${replyCorrelationId} and a tracking correlationId of ${trackingCorrelationId}, which expects a response on a replyTo queue named: ${replyToQueue}.`);
      let response;
      try {
        if (canBindCorrelationIdToCbFunc) {
          response = await _options.withCorrelationId(async () => {
            const resultOfCbFunc = await cbFunc(content);
            return resultOfCbFunc;
          }, trackingCorrelationId);
        } else {
          response = await cbFunc(content);
        }
      } catch (err) {
        response = err;
      }

      // The two approaches used here are fundamentally different. The former, is in response to pubishers that have used the "publishExpectingResponse" approach. This "direct reply-to" approach is a RPC-type approach where we're replying to a requester that is actively waiting and will timeout if we don't reply soon. It makes use of RabbitMQ's special direct reply-to queue. The messages can be short-lived and the queue does not need to be durable. It WILL return errors.
      // The latter approach is in response to publishers that have used the "publish" function with a replyTo option. This is simply for situations when the publisher has specified that the response should be published to an eventname of its choosing. This WON'T publish errors.
      // -- Direct Response --
      if (expectingDirectReply) {
        try {
          await respondDirectlyToRequest(response, replyToQueue, replyCorrelationId, trackingCorrelationId);
        } catch (err) {
          logsEmitter.error(`Failed to respond to a request that expected a direct response. Reason: ${err.message}`);
        }
      // -- Publish Response to another queue --
      } else {
        try {
          if (!(response instanceof Error)) {
            // Technically here we'll be replying to an exchange not a queue. Which may not even have an subscribers in which case the message won't actually end up on a queue
            const pubOptions = {};
            if (trackingCorrelationId) {
              pubOptions.correlationId = trackingCorrelationId;
            } 
            await publish(replyToQueue, response, pubOptions);
          } else {
            logsEmitter.debug('The response was an error, but because the Direct Reply-To approach is not being used in this instance the error will not be added to the event stream.');
          }
        } catch (err) {
          logsEmitter.error('Error whilst trying to publish response (not using a direct reply-to approach)', err);
        }
      }

    //------------------------
    // Not expecting response
    //------------------------
    } else {
      try {
        if (canBindCorrelationIdToCbFunc) {
          await _options.withCorrelationId(async () => {
            await cbFunc(content);
            return;
          }, trackingCorrelationId);
        } else {
          await cbFunc(content);
        } 
      } catch (err) {
        // I only want this logging at the debug level, because the application using this package should be responsible for handling/logging errors that occur within the callback function.
        logsEmitter.debug(`Error occurred whilst processing the subscription handler. This error will not be returned to the publisher, as the publisher is not expecting a response. Error message: ${err.message}`);
      }
    }

    return;
  };

  // Add this subscription to our list so we can add them again if connection ever goes down.
  _subscriptions.push({eventName, cbFunc: cbFuncWithWrapper});

  if (_connected !== true) {
    return Promise.reject(new NoEventStreamConnection('Currently not connected to the event stream'));
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

    // Essentially the type of queue used all comes down to whether the event name has the word 'request' in it when the subscriber starts listening.
    const isRequestQueue = isRequestEvent(exchangeName);
    const queueConfig = isRequestQueue ? _configForRequestQueues : _configForFireForgetQueues;

    return _channel.assertQueue(queueName, queueConfig)
    .then((q) => {

      // Now let's bind this queue to the exchange
      return _channel.bindQueue(q.queue, exchangeName, '')
      .then(() => {
        
        // Tell the server to deliver us any messages in the queue.
        return _channel.consume(q.queue, (msg) => {
          cbFunc(msg);
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
// response can be undefined, a string, a POJO or it can be an Error object in which case the message will have an error type.
function respondDirectlyToRequest(response, replyToQueue, replyCorrelationId, trackingCorrelationId) {

  if (!(check.undefined(response) || check.string(response) || check.object(response) || check.array(response) || check.instance(response, Error))) {
    return Promise.reject(new EventStreamError('The response must be either undefined, a string, a POJO or an Error object.'));
  }

  if (check.not.nonEmptyString(replyCorrelationId)) {
    return Promise.reject(new EventStreamError('replyCorrelationId should be a non-empty string'));
  }  

  if (check.not.nonEmptyString(trackingCorrelationId)) {
    return Promise.reject(new EventStreamError('trackingCorrelationId should be a non-empty string'));
  }

  const errorOccurred = check.instance(response, Error);

  let messageToSend;
  
  if (errorOccurred) {
    messageToSend = {
      name: response.name,
      message: response.message
    };
    if (check.number(response.statusCode)) {
      messageToSend.statusCode = response.statusCode;
    }
  } else {
    messageToSend = response;
  }

  logsEmitter.debug(`Responding to request on queue: ${replyToQueue}, with a reply correlation id: ${replyCorrelationId}, and tracking correlation id: ${trackingCorrelationId}, and response: ${messageToSend}`);

  const bufferToSend = convertToBuffer(messageToSend);

  const sendOptions = {
    correlationId: replyCorrelationId,
    messageId: trackingCorrelationId,
    persistent: false, // no point in persisting response messages
    expiration: 60000 // likewise no point in keeping them in the queue if the original requester doens't handle it straight away.
  };

  if (errorOccurred) {
    sendOptions.type = _typeForErrors;
  }

  // sychronous
  _channel.sendToQueue(replyToQueue, bufferToSend, sendOptions); 

  return Promise.resolve();

}


//-------------------------------------------------
// Convert to buffer
//-------------------------------------------------
// Converts the javascript variable to buffer than
function convertToBuffer(toSend) {

  let toSendStr;

  if (check.undefined(toSend)) {
    // amqplib won't let you send nothing at all as the content, therefore if we want to send nothing we need to send something that implies nothing.
    toSendStr = _noContentString;
  } else if (check.string(toSend)) {
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

  logsEmitter.debug(`Raw message as string: ${msgStr}`);

  // If the string is in JSON format then let's convert it to a POJO
  const isItJson = isJsonString(msgStr);

  if (isItJson) {
    logsEmitter.debug('The message is valid JSON');
    return JSON.parse(msgStr);
  } else if (msgStr === _noContentString) {
    logsEmitter.debug(`The message has no content.`);
    return;
  } else {
    logsEmitter.debug('The message was a string');
    return msgStr;
  }

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
// Is event a request
//-------------------------------------------------
// If the event name contains the word request, e.g. 'deployment.get.request', then it implies it's an event that will be expecting a reply.
function isRequestEvent(eventName) {
  const nameParts = eventName.split('.') ;
  return nameParts.includes('request');
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
  .catch((err) => {
    // Let's throw a custom error here. This will also make sure the .then statements below won't run, which is what want as they won't work with a connection anyway.
    return Promise.reject(new ConnectDuringReconnectFail(`Failed to connect during reconnect. Reason: ${err.message}.`));
  })
  .then(() => {
    logsEmitter.info('Event stream connection reconnected successfully');
    _retrying = false;
    _currentWaitTime = _waitTimes[0];

    // We need to reconnect to the direct reply-to queue too
    return listenToDirectReplyToQueue();
  })
  .then(() => {
    logsEmitter.debug('Listening to the direct reply-to queue again');

    // Now to resubscribe to any subscriptions that were present before the connect was lost.
    logsEmitter.debug(`${_subscriptions.length} subscriptions need to be restablished`);

    if (_subscriptions.length > 0) {

      // Re-establish any subscriptions we had
      return Promise.map(_subscriptions, (sub) => {
        logsEmitter.debug(`About to try reconnecting the ${sub.eventName} subscription`);
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

    logsEmitter.error(`Reconnect issue. Reason: ${err.message}`);

    // If the issue was that we couldn't connect then we'll want to try again in a bit.
    if (err.name === 'ConnectDuringReconnectFail') {
      logsEmitter.info(`Will try to reconnect event stream connection in ${_currentWaitTime} seconds`);
      // Call itself again, the 'bind' here is crucial
      setTimeout(reconnect, _currentWaitTime * 1000);
      updateCurrentWaitTime(); // will make the waitTime longer for next time
    } 

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

