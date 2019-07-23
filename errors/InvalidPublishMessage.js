const EventStreamError = require('./EventStreamError');

module.exports = class InvalidPublishMessage extends EventStreamError {
  constructor(message = 'Invalid publish message') {
    super(message);
    // Ensure the name of this error is the same as the class name.
    this.name = this.constructor.name;
    // Capturing stack trace, excluding constructor call from it.
    Error.captureStackTrace(this, this.constructor);
  }
};