import cron from "cron";
const CronJob = cron.CronJob;

import moment from "moment";
import logger from "../../../logger";
import {
  morning,
  night,
  now,
  pastHour,
  previousDayAtNight,
  TransactionMessagingType,
  TransactionStatus,
} from "../commons/model";
import { PaymentServiceClient } from "../../db";
import type { TransactionMessaging } from "../commons/model";
import { PAYMENT_EMITTER } from "./requery.payment.event";
import paymentEvent from "./requery.payment.event";

function reQueryPendingWalletTopTransfer(callback) {
  const handShakeStatus = "PUBLISHED_SUCCESSFUL";
  const query = {
    text:
      "SELECT * FROM transactions tnx " +
      "WHERE handshake_status = $1 AND tnx.type = $2 " +
      "AND tnx.time_created >= $3 AND tnx.time_created <= $4 ",

    values: [
      handShakeStatus,
      TransactionMessagingType.WALLET_TOP_UP,
      morning(),
      night(),
    ],
  };

  const client = PaymentServiceClient();

  client
    .query(query)
    .then((response) => {
      const results = response.rows;
      logger.info(
        `Total number of queried payment for wallet topup results is [${results.length}]`
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

function reQueryPendingTerminal(callback) {
  const handShakeStatus = "PUBLISHED_COMPLETED";
  const query = {
    text:
      "SELECT * FROM transactions tnx " +
      "JOIN terminals terminalPro ON callback_response -> 'callback_response' ->> 'terminalID' = terminalPro.terminal_id " +
      "WHERE handshake_status != $1 AND tnx.type = $2 ",

    values: [handShakeStatus, TransactionMessagingType.TERMINAL],
  };

  const client = PaymentServiceClient();

  client
    .query(query)
    .then((response) => {
      const results = response.rows;
      logger.info(
        `Total number of queried payment for terminal results is [${results.length}]`
      );
      const transactionMessaging: TransactionMessaging[] = results.map(
        function (data) {
          return handleTerminal(data);
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

export const RetryPaymentWalletTopUpJob = (): CronJob => {
  return new CronJob("0 */5 * * * *", function () {
    const formattedDate = moment.tz("Africa/Lagos");
    logger.info(`::: re-processing for payment started ${formattedDate} :::`);

    reQueryPendingWalletTopTransfer(function (
      transactionMessaging: TransactionMessaging[]
    ) {
      paymentEvent.emit(PAYMENT_EMITTER, transactionMessaging);
    });
  });
};

export const RetryPaymentTerminalJob = (): CronJob => {
  return new CronJob("0 */15 * * * *", function () {
    const formattedDate = moment.tz("Africa/Lagos");
    logger.info(`::: re-processing for payment started ${formattedDate} :::`);

    reQueryPendingTerminal(function (
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
    paymentStatus: data.status,
    email: callbackResponse.customer.email,
    // vendor fucked us up, we had to justapox
    accountNumber: callbackResponse.accountDetails
      ? callbackResponse.accountDetails.accountNumber
      : "",
    vendor: data.vendor,
    type: data.type,
    callbackResponse: data.callback_response,
    timeCreated: data.time_created,
  };
}

function handleTerminal(data): TransactionMessaging {
  return {
    paymentReference: data.payment_reference,
    amount: data.amount,
    paymentStatus: data.status,
    userId: data.user_id,
    terminalId: data.terminal_id,
    walletId: data.wallet_id,
    vendor: data.vendor,
    type: data.type,
    callbackResponse: data.callback_response,
    timeCreated: data.time_created,
  };
}
