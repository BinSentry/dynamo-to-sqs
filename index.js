const assert = require('assert');
const AWS = require('aws-sdk');

const sqs = new AWS.SQS();

const DEFAULT_DYNAMO_EVENT_NAMES = ['INSERT', 'REMOVE', 'MODIFY'];
const RAW_BODY_HANDLER = record => JSON.stringify(record);

class DynamoStreamHandler {
  constructor({ sqsEndpoint, eventTypes: eventNames, logger, customBody } = {}) {
    assert(sqsEndpoint, 'sqsEndpoint is a require paramter');
    this.sqsEndpoint = sqsEndpoint;

    this.eventNames = eventNames ? eventNames.toUpperCase().split(',') : DEFAULT_DYNAMO_EVENT_NAMES;
    assert(
      this.eventNames.every(x => DEFAULT_DYNAMO_EVENT_NAMES.includes(x)),
      `Event Names must be in ${DEFAULT_DYNAMO_EVENT_NAMES}`,
    );

    this.logger = logger ? logger : new ConsoleLogger();
    this.logger.info(`Creating dynamo-to-sqs: SQS Endpoint ${this.sqsEndpoint} | Event Names: ${this.eventNames}`);

    assert(!customBody || {}.toString.call(customBody) === '[object Function]', 'customBody must be a function');
    this.bodyHandler = customBody ? customBody : RAW_BODY_HANDLER;
  }

  async handler(event, context) {
    try {
      const promises = event.Records.map(record => sendToSqs({ record, handler: this }));
      await Promise.all(promises);

      return `Successfully processed ${event.Records.length} records.`;
    } catch (err) {
      this.logger.error({ err }, 'Failed processing records');
      context.fail(err);
    }
  }
}

async function sendToSqs({ record, handler }) {
  handler.logger.info('DynamoDB Record: %j', record);

  const params = {
    MessageBody: handler.bodyHandler(record),
    QueueUrl: handler.sqsEndpoint,
  };

  if (!handler.eventNames.includes(record.eventName.toUpperCase())) {
    handler.logger.info(`Event not forwarded to SQS: Event Name ${record.eventName}`);
    return;
  }

  await sqs.sendMessage(params).promise();
}

class ConsoleLogger {
  info(msg) {
    console.log(msg);
  }
  debug(obj, msg) {
    console.error(obj, msg);
  }
}

module.exports = DynamoStreamHandler;
