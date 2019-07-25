const EventStreamOperationalError = require('./EventStreamOperationalError');

module.exports = class EventStreamResponseError extends EventStreamOperationalError {
  constructor(name, message = 'Error returned by the microservice that handled the event stream request.', statusCode) {
    super(message);
    // Instead of using the name of the class, let's use the name of the error as it appears in the event stream payload.
    this.name = name;
    // Capturing stack trace, excluding constructor call from it.
    Error.captureStackTrace(this, this.constructor);
    this.statusCode = statusCode || 500;
  }
};