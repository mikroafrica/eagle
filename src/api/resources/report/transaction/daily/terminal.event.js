import Events from "events";
import logger from "../../../../../logger";
import type { SlackModel } from "../../../commons/model";

const Emitter = Events.EventEmitter;
const TerminalTransactionEmitter = new Emitter();

const mikroKafkaClient = require("mikro-kafka-client");
const mikroProducer = mikroKafkaClient.mikroProducer;
const KafkaConfig = mikroKafkaClient.kafkaConfig;

export const TERMINAL_EMITTER = "TERMINAL_EMITTER";

TerminalTransactionEmitter.on(TERMINAL_EMITTER, async function (
  payload: string,
  time: string
) {
  try {
    await publishTerminalTransactionSummary(payload, time);
  } catch (e) {}
});

function publishTerminalTransactionSummary(payLoad: string, time: string) {
  return new Promise((resolve, reject) => {
    const config: KafkaConfig = {
      hostname: process.env.KAFKA_HOST,
      username: process.env.KAFKA_USERNAME,
      password: process.env.KAFKA_PASSWORD,
      topic: process.env.KAFKA_SLACK_NOTIFICATION_TOPIC,
    };

    const model: SlackModel = {
      title: `Terminal Report Summary @ ${time}`,
      channel: process.env.REPORT_CHANNEL,
      message: payLoad,
    };
    mikroProducer(config, JSON.stringify(model), function (err, data) {
      if (err) {
        logger.error(
          `error occurred while publishing terminal report to slack [${err}]`
        );
        reject();
      }

      logger.info(`published transaction summary`);

      resolve();
    });
  });
}

export default TerminalTransactionEmitter;
