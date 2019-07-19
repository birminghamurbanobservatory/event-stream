
# Event Stream

An event stream approach is used to simplify communication between microservices. This package provides an interface for interacting with this event stream.

## Initialise

Before you can publish or subscribe you must first initiate the connection to the event stream.

```js
event.init({
  url: '', 
  appName: 'my-microservice'
})
.then(() => {
  // READY FOR ACTION
})
.catch((err) => {
  console.log(err.message);
});
```

## Publish an event

```js
event.publish('time-message', `Time is ${new Date().toISOString()}`)
.then(() => {
  console.log('Published ok');
})
.catch((err) => {
  console.log(`Failed to publish, reason: ${err.message}`);
});
```

N.B. the message to publish should be a string or a plain old javascript object.


## Publish an event and wait for a response

```js
event.publishExpectingResponse('double-numbers', {myNumber: 3})
.then((result) => {
  console.log(`Doubled number: ${result.myNumber}`)
})
.catch((err) => {
  console.log(`Failed to publish, reason: ${err.message}`);
});
```

`event.publishExpectingResponse` returns a promise that resolves with the response from whichever microservice responded the event.


## Subscribe to an event

```js
event.subscribe('double-numbers', async (message) => {
  return message.myNumber * 2;
});
```


# TODOs

- Once you have an Urban Observatory version of the node-logger up on a git repository then use this instead.
- Needs more tests.