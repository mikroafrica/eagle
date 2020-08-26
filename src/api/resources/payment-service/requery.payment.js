import cron from "cron";
const CronJob = cron.CronJob;

import moment from "moment";
import logger from "../../../logger";
import {
  morning,
  night,
  TransactionMessagingType,
  TransactionStatus,
} from "../commons/model";
import { PaymentServiceClient } from "../../db";
import type { TransactionMessaging } from "../commons/model";
import { PAYMENT_EMITTER } from "./requery.payment.event";
import paymentEvent from "./requery.payment.event";

function reQueryPendingTransfer(callback) {
  const handShakeStatus = "PUBLISHED_SUCCESSFUL";
  const query = {
    text:
      "SELECT * FROM transactions tnx " +
      "WHERE handshake_status = $1 AND tnx.status = $2 AND (tnx.type = $3 or tnx.type = $4)" ,
      // "AND tnx.time_created >= $4 AND tnx.time_created <= $5 ",

    values: [
      handShakeStatus,
      TransactionStatus.SUCCESS,
      TransactionMessagingType.WALLET_TOP_UP,
      TransactionMessagingType.TERMINAL,
    ],
  };

  const client = PaymentServiceClient();

  client
    .query(query)
    .then((response) => {
      const results = response.rows;
      logger.info(
        `Total number of queried payment results is [${results.length}]`
      );
      const transactionMessaging: TransactionMessaging[] = results.map(
        function (data) {
          return handleWalletTopUp(data);
        }
      );
      callback(transactionMessaging);

      client.end();
    })
    .catch((error) => {
      logger.error(
        `error occurred while fetching pending with error [${error}]`
      );
    });
}

export const RetryPaymentJob = (): CronJob => {
  return new CronJob("0 */5 * * * *", function () {
    const formattedDate = moment.tz("Africa/Lagos");
    logger.info(`::: re-processing for payment started ${formattedDate} :::`);

    reQueryPendingTransfer(function (
      transactionMessaging: TransactionMessaging[]
    ) {
      paymentEvent.emit(PAYMENT_EMITTER, transactionMessaging);
    });
  });
};

function handleWalletTopUp(data): TransactionMessaging {
  const callbackResponse = data.callback_response.callback_response;
  return {
    paymentReference: data.payment_reference,
    amount: data.amount,
    paymentStatus: TransactionStatus.SUCCESS,
    email: callbackResponse.customer.email,
    accountNumber: callbackResponse.accountDetails.accountNumber,
    vendor: data.vendor,
    type: TransactionMessagingType.WALLET_TOP_UP,
    callbackResponse: callbackResponse,
  };
}
