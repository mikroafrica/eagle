import cron from "cron";

const CronJob = cron.CronJob;

import moment from "moment";

import { TransactionServiceClient } from "../../../../db";
import logger from "../../../../../logger";

import reQueryWalletTopUpEvent, {
  REQUERY_WALLET_TOP_UP_EMITTER,
} from "./requery.wallet-top-up.event";

import {
  billerPurchaseTransactionStatus,
  morning,
  night,
  paymentSuccessfulTransactionStatus,
  pendingTransactionStatus,
  TransactionMessagingType,
  TransactionStatus,
  walletTopUpTransactionType,
} from "../../../commons/model";
import type {
  TransactionMessaging,
  TransactionMessagingContainer,
} from "../../../commons/model";

function reQueryPendingWalletTopUp(callback) {
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
      walletTopUpTransactionType,
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
        `Total number of queried wallet top-up results is [${results.length}]`
      );
      const transactionContainer: TransactionMessagingContainer[] = results.map(
        function (data: TransactionMessaging) {
          const transactionMessaging: TransactionMessaging = {
            paymentReference: data.unique_identifier,
            amount: data.amount,
            // vendor fucked us up, we had to justapox
            accountNumber: data.userdata.meta
              ? data.userdata.meta.accountNumber
              : "",
            paymentStatus: TransactionStatus.SUCCESS,
            email: data.userdata.meta.accountEmail,
            vendor: data.meta.data.vendor,
            type: TransactionMessagingType.WALLET_TOP_UP,
            // callbackResponse: data.gateway_response, this is not necessary to return
            // back to the user
            walletId: data.destination_wallet_id,
            timeCreated: data.time_created,
          };
          const transactionReference = data.transaction_reference;
          return {
            messaging: transactionMessaging,
            transactionReference,
          };
        }
      );
      callback(transactionContainer);

      client.end();
    })
    .catch((error) => {
      logger.error(
        `error occurred while fetching pending wallet topup from transaction service with error [${error}]`
      );
    });
}

export const RetryWalletTopUpJob = (): CronJob => {
  return new CronJob("0 */2 * * * *", function () {
    const formattedDate = moment.tz("Africa/Lagos");
    logger.info(`::: reQuery for wallet top-up started ${formattedDate} :::`);

    reQueryPendingWalletTopUp(function (
      transactionContainer: TransactionMessagingContainer[]
    ) {
      reQueryWalletTopUpEvent.emit(
        REQUERY_WALLET_TOP_UP_EMITTER,
        transactionContainer
      );
    });
  });
};
