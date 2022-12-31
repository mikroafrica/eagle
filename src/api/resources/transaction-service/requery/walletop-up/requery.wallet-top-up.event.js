import type { TransactionMessaging } from "../../../commons/model";
import logger from "../../../../../logger";
import Events from "events";
import async from "async";

const mikroKafkaClient = require("mikro-kafka-client");
const mikroProducer = mikroKafkaClient.mikroProducer;
const KafkaConfig = mikroKafkaClient.kafkaConfig;

const Emitter = Events.EventEmitter;
const ReQueryWalletTopUpEmitter = new Emitter();

export const REQUERY_WALLET_TOP_UP_EMITTER = "REQUERY_WALLET_TOP_UP";

ReQueryWalletTopUpEmitter.on(
  REQUERY_WALLET_TOP_UP_EMITTER,
  function (transactionMessagingList: TransactionMessaging[]) {
    async.forEachOf(
      transactionMessagingList,
      async (transactionMessaging, key, callback) => {
        // push payment dto to kafka for further processing
        try {
          await publishWalletTopUpDto(transactionMessaging);
        } catch (e) {
        } finally {
          callback();
        }
      },
      () => {
        if (transactionMessagingList.length > 0) {
          logger.info(`published all pending wallet top-up for reQuery`);
        }
      }
    );
  }
);

function publishWalletTopUpDto(transactionMessaging: TransactionMessaging) {
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
      transactionMessaging.transactionRef + `${new Date().getTime()}`,
      function (response) {
        logger.info(
          `published reQuery wallet top-up for reference [${
            transactionMessaging.paymentReference
          }] with response [${JSON.stringify(response)}]`
        );
        resolve();
      }
    );
  });
}

export default ReQueryWalletTopUpEmitter;
