const EventStreamError = require('./EventStreamError');

module.exports = class EventStreamOperationalError extends EventStreamError {
  constructor(message = 'An operational event stream error occurred ') {
    super(message);
    // Ensure the name of this error is the same as the class name.
    this.name = this.constructor.name;
    // Capturing stack trace, excluding constructor call from it.
    Error.captureStackTrace(this, this.constructor);
    // Add a statusCode, useful when converting an error object to a HTTP response
    this.statusCode = 500; 
  }
};