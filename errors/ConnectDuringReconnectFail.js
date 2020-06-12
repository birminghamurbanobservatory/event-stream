const EventStreamOperationalError = require('./EventStreamOperationalError');

module.exports = class ConnectDuringReconnectFail extends EventStreamOperationalError {
  constructor(message = 'Failed to connect during the reconnect procedure') {
    super(message);
    // Ensure the name of this error is the same as the class name.
    this.name = this.constructor.name;
    // Capturing stack trace, excluding constructor call from it.
    Error.captureStackTrace(this, this.constructor);
  }
};