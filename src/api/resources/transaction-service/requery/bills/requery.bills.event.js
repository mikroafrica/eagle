import Events from "events";
import type { ReQueryModel } from "../../../commons/model";
import async from "async";
import {
  findByTransactionReference,
  saveTransaction,
  updateByTransactionReference,
} from "../../model/transaction.service";
import logger from "../../../../../logger";

const Emitter = Events.EventEmitter;
const ReQueryEmitter = new Emitter();

const mikroKafkaClient = require("mikro-kafka-client");
const mikroProducer = mikroKafkaClient.mikroProducer;
const KafkaConfig = mikroKafkaClient.kafkaConfig;

export const RE_QUERY_BILL_EMITTER = "RE_QUERY_BILL_EMITTER";

ReQueryEmitter.on(RE_QUERY_BILL_EMITTER, function (
  reQueryModels: ReQueryModel[]
) {
  async.forEachOf(
    reQueryModels,
    async (reQueryModel, key, callback) => {
      const transactionReference = reQueryModel.transactionRef;

      // check if transaction reference exist in this
      let transactionObject = await findByTransactionReference(
        transactionReference
      );

      if (!transactionObject) {
        transactionObject = await saveTransaction(transactionReference);
      }

      // update the transaction with its number of retry count
      const updatedRetryCount = transactionObject.retryCount;
      await updateByTransactionReference(
        transactionReference,
        updatedRetryCount + 1,
        0
      );

      // push payment dto to kafka for further processing
      try {
        await publishReQuery(reQueryModel, transactionReference);
      } catch (e) {
      } finally {
        callback();
      }
    },
    () => {
      if (reQueryModels.length > 0) {
        logger.info(`published all bills for reQuery`);
      }
    }
  );
});

function publishReQuery(reQueryModel: ReQueryModel) {
  return new Promise((resolve, reject) => {
    const config: KafkaConfig = {
      hostname: process.env.KAFKA_HOST,
      username: process.env.KAFKA_USERNAME,
      password: process.env.KAFKA_PASSWORD,
      topic: process.env.KAFKA_BILL_REQUERY_TOPIC,
    };

    mikroProducer(config, JSON.stringify(reQueryModel), function (err, data) {
      if (err) {
        logger.error(`error occurred while publishing transfer [${err}]`);
        reject();
      }

      logger.info(`published reQuery bills [${reQueryModel}]`);

      resolve();
    });
  });
}

export default ReQueryEmitter;
