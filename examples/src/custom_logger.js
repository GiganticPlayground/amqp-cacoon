const { createLogger } = require('logra');

const logger = createLogger('amqp-cacoon-example', {
  level: 'silly',
  style: 'pretty',
});

module.exports = logger;
