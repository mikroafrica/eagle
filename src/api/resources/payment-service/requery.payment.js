import cron from "cron";

const CronJob = cron.CronJob;

import moment from "moment";
import logger from "../../../logger";
import {
  morning,
  night,
  pastMinutes,
  previousDayInMorning,
  TransactionMessagingType,
} from "../commons/model";
import { PaymentServiceClient } from "../../db";
import type { TransactionMessaging } from "../commons/model";
import { PAYMENT_EMITTER } from "./requery.payment.event";
import paymentEvent from "./requery.payment.event";

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

function reQueryPendingWalletTop(callback) {
  const handShakeStatus = "PUBLISHED_COMPLETED";
  const query = {
    text:
      "SELECT * FROM transactions tnx " +
      "WHERE handshake_status != $1 AND tnx.type = $2 " +
      "AND tnx.time_updated >= $3 AND tnx.time_updated <= $4 ",

    values: [
      handShakeStatus,
      TransactionMessagingType.WALLET_TOP_UP,
      previousDayInMorning(),
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

export const RetryPaymentWalletTopUpJob = (): CronJob => {
  return new CronJob("0 */5 * * * *", function () {
    const formattedDate = moment.tz("Africa/Lagos");
    logger.info(
      `::: Wallet top-up from payment db processing for payment started ${formattedDate} :::`
    );

    reQueryPendingWalletTop(function (
      transactionMessaging: TransactionMessaging[]
    ) {
      paymentEvent.emit(PAYMENT_EMITTER, transactionMessaging);
    });
  });
};

/**
 * handle withdrawals with handshake of published_successful
 */

function handleTerminal(data): TransactionMessaging {
  return {
    paymentReference: data.payment_reference,
    amount: data.amount,
    paymentStatus: data.status,
    userId: data.user_id,
    terminalId: data.customer_biller_id,
    walletId: data.wallet_id,
    vendor: data.vendor,
    type: data.type,
    callbackResponse: data.callback_response,
    timeCreated: data.tnxdate,
  };
}

function reQueryPendingTerminal(callback) {
  const completedhandShakeStatus = "PUBLISHED_COMPLETED";

  const pastThreeMinutes = pastMinutes(3);

  // fetch first 20 transactions in ascending order
  const query = {
    text:
      "SELECT *, tnx.time_created as tnxDate FROM transactions tnx " +
      "JOIN terminals terminalPro ON " +
      "tnx.customer_biller_id = terminalPro.terminal_id " +
      "WHERE handshake_status != $1 AND tnx.type = $2 " +
      "AND tnx.time_created >= $3 AND tnx.time_created <= $4 " +
      "ORDER BY tnxDate ASC limit 50",

    values: [
      completedhandShakeStatus,
      TransactionMessagingType.TERMINAL,
      previousDayInMorning(),
      pastThreeMinutes,
    ],
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

export const RetryPaymentTerminalJob = (): CronJob => {
  return new CronJob("0 */3 * * * *", function () {
    const formattedDate = moment.tz("Africa/Lagos");
    logger.info(`::: re-processing for payment started ${formattedDate} :::`);

    reQueryPendingTerminal(function (
      transactionMessaging: TransactionMessaging[]
    ) {
      paymentEvent.emit(PAYMENT_EMITTER, transactionMessaging);
    });
  });
};
