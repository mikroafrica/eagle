import type { TransactionMessaging } from "../../../commons/model";

import logger from "../../../../../logger";
import Events from "events";
import async from "async";

const mikroKafkaClient = require("mikro-kafka-client");
const mikroProducer = mikroKafkaClient.mikroProducer;
const KafkaConfig = mikroKafkaClient.kafkaConfig;

const Emitter = Events.EventEmitter;
const ReQueryWithdrawalEmitter = new Emitter();

export const REQUERY_WITHDRAWAL_EMITTER = "REQUERY_WITHDRAWAL";

ReQueryWithdrawalEmitter.on(REQUERY_WITHDRAWAL_EMITTER, function (
  transactionMessagingList: TransactionMessaging[]
) {
  async.forEachOf(
    transactionMessagingList,
    async (transactionMessaging, key, callback) => {
      // push terminal dto to kafka for further processing
      try {
        await publishTerminalDto(transactionMessaging);
      } catch (e) {
      } finally {
        callback();
      }
    },
    () => {
      if (transactionMessagingList.length > 0) {
        logger.info(`published all pending withdrawal for reQuery`);
      }
    }
  );
});

function publishTerminalDto(transactionMessaging: TransactionMessaging) {
  return new Promise((resolve, reject) => {
    const config: KafkaConfig = {
      hostname: process.env.KAFKA_HOST,
      username: process.env.KAFKA_USERNAME,
      password: process.env.KAFKA_PASSWORD,
      topic: process.env.KAFKA_TRANSACTION_TOPIC,
      groupId: process.env.KAFKA_CLUSTER_ID,
    };

    mikroProducer(
      config,
      transactionMessaging,
      transactionMessaging.paymentReference,
      function (response) {
        logger.info(
          `published reQuery withdrawal for reference [${
            transactionMessaging.paymentReference
          }] with response [${JSON.stringify(response)}]`
        );
        resolve();
      }
    );
  });
}

export default ReQueryWithdrawalEmitter;
