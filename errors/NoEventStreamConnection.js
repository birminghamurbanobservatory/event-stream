const EventStreamOperationalError = require('./EventStreamOperationalError');

module.exports = class NoEventStreamConnection extends EventStreamOperationalError {
  constructor(message = 'Currently unconnected from the event stream') {
    super(message);
    // Ensure the name of this error is the same as the class name.
    this.name = this.constructor.name;
    // Capturing stack trace, excluding constructor call from it.
    Error.captureStackTrace(this, this.constructor);
  }
};