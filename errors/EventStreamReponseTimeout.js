const EventStreamOperationalError = require('./EventStreamOperationalError');

module.exports = class EventStreamResponseTimeout extends EventStreamOperationalError {
  constructor(message = 'Timed out waiting for response from event stream') {
    super(message);
    // Ensure the name of this error is the same as the class name.
    this.name = this.constructor.name;
    // Capturing stack trace, excluding constructor call from it.
    Error.captureStackTrace(this, this.constructor);
  }
};