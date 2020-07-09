import Events from "events";
import logger from "../../../../../logger";
import type { SlackModel } from "../../../commons/model";

const Emitter = Events.EventEmitter;
const TransactionSummaryEmitter = new Emitter();

const mikroKafkaClient = require("mikro-kafka-client");
const mikroProducer = mikroKafkaClient.mikroProducer;
const KafkaConfig = mikroKafkaClient.kafkaConfig;

export const TRANSACTION_SUMMARY_EMITTER = "TRANSACTION_EMITTER";

TransactionSummaryEmitter.on(TRANSACTION_SUMMARY_EMITTER, async function (
  payload: string,
  time: string
) {
  try {
    await publishTransactionSummary(payload, time);
  } catch (e) {}
});

function publishTransactionSummary(payLoad: string, time: string) {
  return new Promise((resolve, reject) => {
    const config: KafkaConfig = {
      hostname: process.env.KAFKA_HOST,
      username: process.env.KAFKA_USERNAME,
      password: process.env.KAFKA_PASSWORD,
      topic: process.env.KAFKA_SLACK_NOTIFICATION_TOPIC,
    };

    const model: SlackModel = {
      title: `Transaction Summary btw ${time}`,
      channel: process.env.REPORT_CHANNEL,
      message: payLoad,
    };
    mikroProducer(config, JSON.stringify(model), function (err, data) {
      if (err) {
        logger.error(
          `error occurred while publishing transaction summary to slack [${err}]`
        );
        reject();
      }

      logger.info(`published transaction summary`);

      resolve();
    });
  });
}

export default TransactionSummaryEmitter;
