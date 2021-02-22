import async from "async";
import {
  findByTransactionReference,
  saveTransaction,
  updateByTransactionReference,
} from "../../model/transaction.service";
import logger from "../../../../../logger";
import Events from "events";
import { PaymentType } from "./retry.transfer";
import type { PaymentDto } from "../../../commons/model";

const Emitter = Events.EventEmitter;
const ReQueryEmitter = new Emitter();

const mikroKafkaClient = require("mikro-kafka-client");
const mikroProducer = mikroKafkaClient.mikroProducer;
const KafkaConfig = mikroKafkaClient.kafkaConfig;

export const REQUERY_TRANSACTION_EMITTER = "REQUERY_TRANSACTION";

ReQueryEmitter.on(REQUERY_TRANSACTION_EMITTER, function (
  paymentDtoList: PaymentDto[]
) {
  async.forEachOf(
    paymentDtoList,
    async (paymentDto, key, callback) => {
      const transactionReference = paymentDto.transactionRef;

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

      if (transactionObject.retryCount >= 1) {
        logger.info(
          `::: transaction reference logged for re-processing [${transactionReference}] :::`
        );
        updatedRetryCount = 0;
        updatedReProcessCount = updatedReProcessCount + 1;

        paymentDto = Object.assign(paymentDto, {
          paymentType: PaymentType.BANK_TRANSFER_REPROCESS,
        });
      } else {
        updatedRetryCount = updatedRetryCount + 1;
      }

      // update the transaction with its number of retry count
      await updateByTransactionReference(
        transactionReference,
        updatedRetryCount,
        updatedReProcessCount
      );

      // push payment dto to kafka for further processing
      try {
        await publishPaymentDto(paymentDto, transactionReference);
      } catch (e) {
      } finally {
        callback();
      }
    },
    () => {
      if (paymentDtoList.length > 0) {
        logger.info(`published all transfers for reQuery`);
      }
    }
  );
});

function publishPaymentDto(
  paymentDto: PaymentDto,
  transactionReference: string
) {
  return new Promise((resolve, reject) => {
    const config: KafkaConfig = {
      hostname: process.env.KAFKA_HOST,
      username: process.env.KAFKA_USERNAME,
      password: process.env.KAFKA_PASSWORD,
      topic: process.env.KAFKA_PAYMENT_PAYLOAD_TOPIC,
    };

    mikroProducer(config, JSON.stringify(paymentDto), function (err, data) {
      if (err) {
        logger.error(`error occurred while publishing transfer [${err}]`);
        reject();
      }

      logger.info(
        `published reQuery transfer for reference [${transactionReference}]`
      );

      resolve();
    });
  });
}

export default ReQueryEmitter;
