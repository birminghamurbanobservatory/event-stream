const EventStreamError = require('./EventStreamError');

module.exports = class InvalidPublishBody extends EventStreamError {
  constructor(message = 'Invalid publish body') {
    super(message);
    // Ensure the name of this error is the same as the class name.
    this.name = this.constructor.name;
    // Capturing stack trace, excluding constructor call from it.
    Error.captureStackTrace(this, this.constructor);
  }
};