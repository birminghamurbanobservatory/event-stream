const EventStreamError = require('./EventStreamError');

module.exports = class InvalidEventName extends EventStreamError {
  constructor(message = 'Invalid event name') {
    super(message);
    // Ensure the name of this error is the same as the class name.
    this.name = this.constructor.name;
    // Capturing stack trace, excluding constructor call from it.
    Error.captureStackTrace(this, this.constructor);
  }
};