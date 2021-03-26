import cron from "cron";

const CronJob = cron.CronJob;

import moment from "moment";
import { TransactionServiceClient } from "../../../../db";
import logger from "../../../../../logger";

import reQueryWithdrawalEmitter, {
  REQUERY_WITHDRAWAL_EMITTER,
} from "./requery.withdrawal-event";

import {
  billerPurchaseTransactionStatus,
  night,
  paymentSuccessfulTransactionStatus,
  pendingPaymentReversalStatus,
  pendingTransactionStatus,
  previousDayInMorning,
  TransactionMessagingType,
  TransactionStatus,
  withdrawalTransactionType,
} from "../../../commons/model";
import type { TransactionMessaging } from "../../../commons/model";

async function reQueryPendingWithdrawalWalletTopUp() {
  const query = {
    text:
      "SELECT * FROM transactions tnx " +
      "JOIN transaction_statuses status ON status.id = tnx.transaction_status " +
      "JOIN transaction_types type ON type.id = tnx.transaction_type " +
      "WHERE (status.name = $1 OR status.name = $2 OR status.name = $3 OR status.name = $4) " +
      "AND type.name = $5 AND tnx.time_updated >= $6 AND tnx.time_updated <= $7 AND tnx.user_id != 'unAssigned' " +
      "ORDER BY tnx.time_updated ASC limit 100 ",

    values: [
      pendingTransactionStatus,
      paymentSuccessfulTransactionStatus,
      billerPurchaseTransactionStatus,
      pendingPaymentReversalStatus,
      withdrawalTransactionType,
      previousDayInMorning(),
      night(),
    ],
  };

  const pool = TransactionServiceClient();
  try {
    const client = await pool.connect();
    const response = await client.query(query.text, query.values);

    const results = response.rows;

    logger.info(
      `Total number of queried withdrawal results is [${results.length}]`
    );
    const transactionMessaging: TransactionMessaging = results.map(function (
      data
    ) {
      let status = TransactionStatus.PENDING;
      if (data.transaction_status === "5" || data.transaction_status === "3") {
        status = TransactionStatus.SUCCESS;
      }

      if (data.transaction_status === "10") {
        status = TransactionStatus.REVERSAL;
      }

      return {
        paymentReference: data.unique_identifier,
        amount: data.amount,
        vendor: data.vendor,
        paymentStatus: status,
        type: TransactionMessagingType.TERMINAL,
        terminalId: data.customer_biller_id,
        // callbackResponse: data.gateway_response, this is not necessary to return
        // back to the user
        userId: data.user_id,
        walletId: data.destination_wallet_id,
        timeCreated: data.time_created,
      };
    });
    pool.end();
    return Promise.resolve(transactionMessaging);
  } catch (e) {
    pool.end();
    return Promise.reject(e);
  }
}

export const RetryWithdrawalJob = (): CronJob => {
  return new CronJob("0 */2 * * * *", function () {
    const formattedDate = moment.tz("Africa/Lagos");
    logger.info(`::: reQuery for withdrawal started ${formattedDate} :::`);

    reQueryPendingWithdrawalWalletTopUp()
      .then((transactionMessaging) => {
        reQueryWithdrawalEmitter.emit(
          REQUERY_WITHDRAWAL_EMITTER,
          transactionMessaging
        );
      })
      .catch((err) => {
        logger.error(`error occurred while publishing result: ${err} `);
      });
  });
};
