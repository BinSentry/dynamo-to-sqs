const assert = require('assert');
const AWS = require('aws-sdk');

const sqs = new AWS.SQS();

const DEFAULT_DYNAMO_EVENT_NAMES = ['INSERT', 'REMOVE', 'MODIFY'];
const RAW_BODY_HANDLER = record => record;

class DynamoStreamHandler {
  constructor({ sqsEndpoint, eventNames, logger, customBodyHandler } = {}) {
    assert(sqsEndpoint, 'sqsEndpoint is a require paramter');
    this.sqsEndpoint = sqsEndpoint;

    this.eventNames = eventNames ? eventNames.map(name => name.toUpperCase()) : DEFAULT_DYNAMO_EVENT_NAMES;
    assert(
      this.eventNames.every(x => DEFAULT_DYNAMO_EVENT_NAMES.includes(x)),
      `Event Names must be in ${DEFAULT_DYNAMO_EVENT_NAMES}`,
    );

    this.logger = logger ? logger : new ConsoleLogger();
    this.logger.info(`Creating dynamo-to-sqs: SQS Endpoint ${this.sqsEndpoint} | Event Names: ${this.eventNames}`);

    assert(
      !customBodyHandler || {}.toString.call(customBodyHandler) === '[object Function]',
      'customBody must be a function',
    );
    this.bodyHandler = customBodyHandler ? customBodyHandler : RAW_BODY_HANDLER;

    const params = this;
    this.handler = async (event, context) => {
      try {
        const promises = event.Records.map(record => sendToSqs({ record, params }));
        await Promise.all(promises);

        return `Successfully processed ${event.Records.length} records.`;
      } catch (err) {
        params.logger.error({ err }, 'Failed processing records');
        context.fail(err);
      }
    };
  }
}

async function sendToSqs({ record, params }) {
  params.logger.info('DynamoDB Record: %j', record);

  const body = {
    MessageBody: JSON.stringify(params.bodyHandler(record)),
    QueueUrl: params.sqsEndpoint,
  };

  if (!params.eventNames.includes(record.eventName.toUpperCase())) {
    params.logger.info(`Event not forwarded to SQS: Event Name ${record.eventName}`);
    return;
  }

  await sqs.sendMessage(body).promise();
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
