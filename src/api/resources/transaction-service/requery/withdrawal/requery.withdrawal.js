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
  morning,
  night,
  paymentSuccessfulTransactionStatus,
  pendingTransactionStatus,
  TransactionMessagingType,
  TransactionStatus,
  withdrawalTransactionType,
} from "../../../commons/model";
import type { TransactionMessaging } from "../../../commons/model";

function reQueryPendingWithdrawalWalletTopUp(callback) {
  const query = {
    text:
      "SELECT * FROM transactions tnx " +
      "JOIN transaction_statuses status ON status.id = tnx.transaction_status " +
      "JOIN transaction_types type ON type.id = tnx.transaction_type " +
      "WHERE (status.name = $1 OR status.name = $2 OR status.name = $3) " +
      "AND type.name = $4 " +
      "AND tnx.time_created >= $5 AND tnx.time_created <= $6 ",

    values: [
      pendingTransactionStatus,
      paymentSuccessfulTransactionStatus,
      billerPurchaseTransactionStatus,
      withdrawalTransactionType,
      morning(),
      night(),
    ],
  };

  const client = TransactionServiceClient();

  client
    .query(query)
    .then((response) => {
      const results = response.rows;

      logger.info(
        `Total number of queried withdrawal results is [${results.length}]`
      );
      const transactionMessaging: TransactionMessaging = results.map(function (
        data
      ) {
        return {
          paymentReference: data.unique_identifier,
          amount: data.amount,
          vendor: data.vendor,
          paymentStatus: TransactionStatus.SUCCESS,
          type: TransactionMessagingType.TERMINAL,
          terminalId: data.customer_biller_id,
          // callbackResponse: data.gateway_response, this is not necessary to return back to the user
          userId: data.user_id,
          walletId: data.destination_wallet_id,
          timeCreated: data.time_created,
        };
      });
      callback(transactionMessaging);

      client.end();
    })
    .catch((error) => {
      logger.error(
        `error occurred while fetching pending withdrawal transaction on transaction service level with error [${error}]`
      );
    });
}

export const RetryWithdrawalJob = (): CronJob => {
  return new CronJob("0 */2 * * * *", function () {
    const formattedDate = moment.tz("Africa/Lagos");
    logger.info(`::: reQuery for withdrawal started ${formattedDate} :::`);

    reQueryPendingWithdrawalWalletTopUp(function (
      transactionMessaging: TransactionMessaging
    ) {
      reQueryWithdrawalEmitter.emit(
        REQUERY_WITHDRAWAL_EMITTER,
        transactionMessaging
      );
    });
  });
};
