import Events from "events";
import async from "async";
import logger from "../../../logger";
import type { TransactionMessaging } from "../commons/model";

const mikroKafkaClient = require("mikro-kafka-client");
const mikroProducer = mikroKafkaClient.mikroProducer;
const KafkaConfig = mikroKafkaClient.kafkaConfig;

const Emitter = Events.EventEmitter;
const PaymentEmitter = new Emitter();

export const PAYMENT_EMITTER = "PAYMENT_EMITTER";

PaymentEmitter.on(PAYMENT_EMITTER, function (
  transactionMessagingList: TransactionMessaging[]
) {
  async.forEachOf(
    transactionMessagingList,
    async (transactionMessaging, key, callback) => {
      // push payment to transaction service again for further processing
      try {
        await publishPayment(transactionMessaging);
      } catch (e) {
      } finally {
        callback();
      }
    },
    () => {
      if (transactionMessagingList.length > 0) {
        logger.info(`published all incomplete payment for republishing`);
      }
    }
  );
});

function publishPayment(transactionMessaging: TransactionMessaging) {
  return new Promise((resolve, reject) => {
    const config: KafkaConfig = {
      hostname: process.env.KAFKA_HOST,
      username: process.env.KAFKA_USERNAME,
      password: process.env.KAFKA_PASSWORD,
      topic: process.env.KAFKA_VENDOR_PAYMENT_TOPIC,
    };

    mikroProducer(config, JSON.stringify(transactionMessaging), function (
      err,
      data
    ) {
      if (err) {
        logger.error(`error occurred while publishing payment [${err}]`);
        reject();
      }

      logger.info(
        `published payment with reference [${transactionMessaging.paymentReference}] of type [${transactionMessaging.type}]`
      );

      resolve();
    });
  });
}

export default PaymentEmitter;
