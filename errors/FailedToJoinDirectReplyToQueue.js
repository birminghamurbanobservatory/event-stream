const EventStreamOperationalError = require('./EventStreamOperationalError');

module.exports = class FailedToJoinDirectReplyToQueue extends EventStreamOperationalError {
  constructor(message = 'Failed to join the direct reply-to queue') {
    super(message);
    // Ensure the name of this error is the same as the class name.
    this.name = this.constructor.name;
    // Capturing stack trace, excluding constructor call from it.
    Error.captureStackTrace(this, this.constructor);
  }
};