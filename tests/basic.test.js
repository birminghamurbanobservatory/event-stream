//-------------------------------------------------
// Dependencies
//-------------------------------------------------
const event = require('../index');

//-------------------------------------------------
// Tests
//-------------------------------------------------
describe('Publish tests', () => {

  test('Expect publish to be a function', () => {
    expect(typeof event.publish).toBe('function');
  });

  test('Expect publish to return rejected promise when message is a number', () => {
    expect.assertions(1);
    return event.publish('event-name', 5)
    .catch((err) => {
      expect(err).toBeInstanceOf(Error);
    });
  });
   
});