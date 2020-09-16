import Events from "events";
import type { BillingModel, ReQueryModel } from "../../../commons/model";
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
  billingModels: BillingModel[]
) {
  async.forEachOf(
    billingModels,
    async (billingModel, key, callback) => {
      const transactionReference = billingModel.transactionReference;

      // check if transaction reference exist in this
      let transactionObject = await findByTransactionReference(
        transactionReference
      );

      if (!transactionObject) {
        transactionObject = await saveTransaction(transactionReference);
      }

      /*
        once retry count is greater than 0, the payment should
        be reprocessed and count should be set back to 0. once this is done, the
        transaction is expected to reQuery from 0 to 11 before being reprocessed all over again
       */
      let updatedRetryCount = transactionObject.retryCount;
      let updatedReProcessCount = transactionObject.reProcessCount || 0;

      if (transactionObject.retryCount >= 4) {
        logger.info(
          `::: transaction reference logged for re-processing [${transactionReference}] :::`
        );
        updatedRetryCount = 0;
        updatedReProcessCount = updatedReProcessCount + 1;

        try {
          await publishBillReprocessing(billingModel);
        } catch (e) {
          logger.error(
            `::: processing bills failed with error [${JSON.stringify(e)}] :::`
          );
        }
      } else {
        updatedRetryCount = updatedRetryCount + 1;
      }

      // update the transaction with its number of retry count
      await updateByTransactionReference(
        transactionReference,
        updatedRetryCount,
        updatedReProcessCount
      );

      const reQueryModel: ReQueryModel = {
        vendor: billingModel.vendor,
        transactionReference: billingModel.transactionReference,
      };

      // push payment dto to kafka for further processing
      try {
        await publishReQuery(reQueryModel, transactionReference);
      } catch (e) {
      } finally {
        callback();
      }
    },
    () => {
      if (billingModels.length > 0) {
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
      logger.info(`published reQuery bills [${JSON.stringify(reQueryModel)}]`);

      resolve();
    });
  });
}

function publishBillReprocessing(billModel: BillingModel) {
  return new Promise((resolve, reject) => {
    const config: KafkaConfig = {
      hostname: process.env.KAFKA_HOST,
      username: process.env.KAFKA_USERNAME,
      password: process.env.KAFKA_PASSWORD,
      topic: process.env.KAFKA_BILL_TOPIC,
    };

    mikroProducer(config, JSON.stringify(billModel), function (err, data) {
      if (err) {
        logger.error(`error occurred while publishing transfer [${err}]`);
        reject();
      }

      logger.info(
        `::: published bills processing [${JSON.stringify(billModel)}] :::`
      );

      resolve();
    });
  });
}

export default ReQueryEmitter;
