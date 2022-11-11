import type { TransactionMessagingContainer } from "../../../commons/model";
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
  function (transactionContainerList: TransactionMessagingContainer[]) {
    async.forEachOf(
      transactionContainerList,
      async (transactionContainer, key, callback) => {
        // push payment dto to kafka for further processing
        try {
          await publishWalletTopUpDto(transactionContainer);
        } catch (e) {
        } finally {
          callback();
        }
      },
      () => {
        if (transactionContainerList.length > 0) {
          logger.info(`published all pending wallet top-up for reQuery`);
        }
      }
    );
  }
);

function publishWalletTopUpDto(
  transactionContainer: TransactionMessagingContainer
) {
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
      transactionContainer.messaging,
      transactionContainer.transactionReference,
      function (response) {
        logger.info(
          `published reQuery wallet top-up for reference [${
            transactionContainer.transactionReference
          }] with response [${JSON.stringify(response)}]`
        );
        resolve();
      }
    );
  });
}

export default ReQueryWalletTopUpEmitter;
