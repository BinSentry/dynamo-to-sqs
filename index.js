const assert = require('assert');
const AWS = require('aws-sdk');
Aws.config.update({
  region: process.env.AWS_REGION || 'us-east-1',
  sqs: { apiVersion: '2012-11-05' },
  httpOptions: { connectTimeout: 5000 },
});

const sqs = new AWS.SQS();

const DEFAULT_DYNAMO_EVENT_NAMES = ['INSERT', 'REMOVE', 'MODIFY'];
const RAW_BODY_HANDLER = record => record;

class DynamoStreamHandler {
  constructor({ sqsConfigs, logger, customBodyHandler } = {}) {
    assert(
      sqsConfigs && Array.isArray(sqsConfigs) && sqsConfigs.length > 0,
      'sqsConfig must be an array with at least one element',
    );
    this.sqsConfigs = sqsConfigs;
    this.logger = logger ? logger : new ConsoleLogger();
    const params = this;

    this.sqsConfigs.forEach(setEventNames);
    this.logger.info(`Creating dynamo-to-sqs`);
    this.sqsConfigs.forEach(sqsConfig =>
      params.logger.info(`SQS Endpoint ${sqsConfig.endpoint} | Event Names: ${sqsConfig.eventNames}`),
    );

    assert(
      !customBodyHandler || {}.toString.call(customBodyHandler) === '[object Function]',
      'customBody must be a function',
    );
    this.bodyHandler = customBodyHandler ? customBodyHandler : RAW_BODY_HANDLER;

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

function setEventNames(sqsConfig) {
  const { eventNames } = sqsConfig;
  sqsConfig.eventNames = eventNames ? eventNames.map(name => name.toUpperCase()) : DEFAULT_DYNAMO_EVENT_NAMES;
  assert(
    sqsConfig.eventNames.every(x => DEFAULT_DYNAMO_EVENT_NAMES.includes(x)),
    `Event Names must be in ${DEFAULT_DYNAMO_EVENT_NAMES}`,
  );
}

async function sendToSqs({ record, params }) {
  const MessageBody = JSON.stringify(params.bodyHandler(record));

  params.logger.info('DynamoDB Record: %j', record);

  const promises = params.sqsConfigs.map(sqsConfig => {
    const body = {
      MessageBody,
      QueueUrl: sqsConfig.endpoint,
    };

    if (!sqsConfig.eventNames.includes(record.eventName.toUpperCase())) {
      params.logger.info(`Event not forwarded to SQS ${sqsConfig.endpoint}: Event Name ${record.eventName}`);
      return;
    }

    return sqs.sendMessage(body).promise();
  });

  return Promise.all(promises);
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
