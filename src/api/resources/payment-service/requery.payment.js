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
import { PaymentServiceClient, TransactionServiceClient } from "../../db";
import type { TransactionMessaging } from "../commons/model";
import { PAYMENT_EMITTER } from "./requery.payment.event";
import paymentEvent from "./requery.payment.event";

function handleWalletTopUp(data): TransactionMessaging {
  const callbackResponse = data.callback_response.callback_response;
  return {
    paymentReference: data.payment_reference,
    amount: data.amount,
    paymentStatus: data.status,
    email: callbackResponse.customer ? callbackResponse.customer.email : "",
    // vendor fucked us up, we had to justapox
    accountNumber: data.customer_biller_id,
    vendor: data.vendor,
    type: data.type,
    callbackResponse: data.callback_response,
    timeCreated: data.time_created,
  };
}

async function reQueryPendingWalletTopAndUSSD() {
  const handShakeStatus = "PUBLISHED_COMPLETED";
  const query = {
    text:
      "SELECT * FROM transactions tnx " +
      "WHERE handshake_status != $1 AND (tnx.type = $2 OR tnx.type = $3) " +
      "AND tnx.time_updated >= $4 AND tnx.time_updated <= $5 ",

    values: [
      handShakeStatus,
      TransactionMessagingType.WALLET_TOP_UP,
      TransactionMessagingType.USSD_WITHDRAWAL,
      previousDayInMorning(),
      night(),
    ],
  };

  const pool = PaymentServiceClient();
  try {
    const client = await pool.connect();
    const response = await client.query(query.text, query.values);
    const results = response.rows;

    logger.info(
      `Total number of queried payment for wallet topup results is [${results.length}]`
    );
    const transactionMessaging: TransactionMessaging[] = results.map(function (
      data
    ) {
      return handleWalletTopUp(data);
    });
    pool.end();
    return Promise.resolve(transactionMessaging);
  } catch (e) {
    pool.end();
    return Promise.reject(e);
  }
}

export const RetryPaymentWalletTopAndUSSDJob = (): CronJob => {
  return new CronJob("0 */5 * * * *", function () {
    const formattedDate = moment.tz("Africa/Lagos");
    logger.info(
      `::: Wallet top-up from payment db processing for payment started ${formattedDate} :::`
    );

    reQueryPendingWalletTopAndUSSD()
      .then((transactionMessaging) => {
        paymentEvent.emit(PAYMENT_EMITTER, transactionMessaging);
      })
      .catch((err) => {
        logger.error(`error occurred while publishing result: ${err} `);
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

async function reQueryPendingTerminal() {
  const completedhandShakeStatus = "PUBLISHED_COMPLETED";

  const pastThreeMinutes = pastMinutes(3);

  // fetch first 20 transactions in ascending order
  const query = {
    text:
      "SELECT *, tnx.time_created as tnxDate FROM transactions tnx " +
      "JOIN terminals terminalPro ON " +
      "tnx.customer_biller_id = terminalPro.terminal_id " +
      "WHERE handshake_status != $1 AND tnx.type = $2 " +
      "AND tnx.time_updated >= $3 AND tnx.time_updated <= $4 " +
      "ORDER BY tnxDate DESC limit 70",

    values: [
      completedhandShakeStatus,
      TransactionMessagingType.TERMINAL,
      previousDayInMorning(),
      pastThreeMinutes,
    ],
  };

  const pool = PaymentServiceClient();
  const client = await pool.connect();
  const response = await client.query(query.text, query.values);
  const results = response.rows;

  logger.info(
    `Total number of queried payment for terminal results is [${results.length}]`
  );
  const transactionMessaging: TransactionMessaging[] = results.map(function (
    data
  ) {
    return handleTerminal(data);
  });
  pool.end();
  return Promise.resolve(transactionMessaging);
}

export const RetryPaymentTerminalJob = (): CronJob => {
  return new CronJob("0 */4 * * * *", function () {
    const formattedDate = moment.tz("Africa/Lagos");
    logger.info(`::: re-processing for payment started ${formattedDate} :::`);

    reQueryPendingTerminal()
      .then((transactionMessaging) => {
        paymentEvent.emit(PAYMENT_EMITTER, transactionMessaging);
      })
      .catch((err) => {
        logger.error(`error occurred while publishing result: ${err} `);
      });
  });
};
