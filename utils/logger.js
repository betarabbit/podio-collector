const winston = require('winston')

module.exports = new winston.Logger({
  transports: [
    new winston.transports.Console({
      timestamp: () => new Date(),
      level: process.env.LOG_LEVEL || 'debug',
      colorize: true
    })
  ]
})
