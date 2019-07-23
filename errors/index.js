const requireDirectory = require('require-directory');

// Exports an object, there will be a property named after every file in this directory that's not a test file, e.g. InvalidPublishMessage.js becomes the property InvalidPublishMessage, the value of which is whatever InvalidPublishMessage.js exports.
module.exports = requireDirectory(module, {
  exclude: /.test.js$/
});



