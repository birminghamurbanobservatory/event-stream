const EventStreamResponseTimeout = require('./EventStreamResponseTimeout');
const EventStreamOperationalError = require('./EventStreamOperationalError');

//-------------------------------------------------
// Tests
//-------------------------------------------------
describe('Check EventStreamResponseTimeout', () => {

  test('EventStreamResponseTimeout is an instance of Error', () => {
    expect(new EventStreamResponseTimeout('Whoops')).toBeInstanceOf(Error);
  });

  test('EventStreamResponseTimeout is an instance of EventStreamOperationalError', () => {
    expect(new EventStreamResponseTimeout('Whoops')).toBeInstanceOf(EventStreamOperationalError);
  }); 
 
  test('It has the correct name property', () => {
    const exampleError = new EventStreamResponseTimeout('Whoops');
    expect(exampleError.name).toBe('EventStreamResponseTimeout');
  });    

  test('Sets a default message when left undefined', () => {
    const exampleError = new EventStreamResponseTimeout();
    expect(typeof exampleError.message).toBe('string');
    expect(exampleError.message.length).toBeGreaterThan(0);
  });  

  test('Applies a custom message', () => {
    const msg = 'Whoops';
    const exampleError = new EventStreamResponseTimeout(msg);
    expect(exampleError.message).toBe(msg);
  });

});
