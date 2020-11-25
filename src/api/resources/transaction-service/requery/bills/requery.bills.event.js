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

      const reQueryModel: ReQueryModel = {
        vendor: billingModel.vendor,
        transactionReference: billingModel.transactionReference,
      };

      logger.info(
        `published bill reQuery Model [${JSON.stringify(reQueryModel)}]`
      );

      try {
        // push payment dto to kafka for requering
        await publishReQuery(reQueryModel, transactionReference);
      } catch (e) {
        logger.error(
          `::: reQuery bills failed with error [${JSON.stringify(e)}] :::`
        );
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
        `::: published bills processing [${JSON.stringify(
          billModel.transactionReference
        )}] :::`
      );

      resolve();
    });
  });
}

export default ReQueryEmitter;
