import cron from "cron";
const CronJob = cron.CronJob;

import moment from "moment";

import logger from "../../../../../logger";
import { TransactionServiceClient } from "../../../../db";

import reQueryBillEvent from "./requery.bills.event";
import { RE_QUERY_BILL_EMITTER } from "./requery.bills.event";
import {
  airtimeTransactionType,
  billerPurchaseTransactionStatus,
  cableTransactionType,
  dataTransactionType,
  morning,
  night,
  paymentSuccessfulTransactionStatus,
  pendingTransactionStatus,
  phcnTransactionType,
} from "../../../commons/model";
import type { BillingModel } from "../../../commons/model";

function reQueryPendingBills(callback) {
  const query = {
    text:
      "SELECT * FROM transactions tnx " +
      "JOIN transaction_statuses status ON status.id = tnx.transaction_status " +
      "JOIN transaction_types type ON type.id = tnx.transaction_type " +
      "WHERE (status.name = $1 OR status.name = $2 OR status.name = $3) " +
      "AND (type.name = $4 OR type.name = $5 OR type.name = $6 OR type.name = $7) " +
      "AND tnx.time_created >= $8 AND tnx.time_created <= $9 ",

    values: [
      pendingTransactionStatus,
      paymentSuccessfulTransactionStatus,
      billerPurchaseTransactionStatus,
      airtimeTransactionType,
      phcnTransactionType,
      cableTransactionType,
      dataTransactionType,
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
        `Total number of queried bills results is [${results.length}]`
      );
      const billingModels: BillingModel[] = results.map(function (data) {
        return {
          transactionReference: data.transaction_reference,
          vendor: data.vendor,
          phoneNumber:
            data.meta.data.customerPhoneNumber || data.customer_biller_id,
          amount: data.amount,
          productId: data.meta.data.productId,
          meterNumber: data.customer_biller_id,
          type: data.name,
          smartCardNumber: data.customer_biller_id,
          category: data.name,
        };
      });
      callback(billingModels);

      client.end();
    })
    .catch((error) => {
      logger.error(
        `error occurred while fetching pending with error [${error}]`
      );
    });
}

// run job every three minutes
export const RetryBillsJob = (): CronJob => {
  return new CronJob("0 */3 * * * *", function () {
    const formattedDate = moment.tz("Africa/Lagos");
    logger.info(`::: reQuery for bills started ${formattedDate} :::`);

    reQueryPendingBills(function (billingModels: BillingModel[]) {
      reQueryBillEvent.emit(RE_QUERY_BILL_EMITTER, billingModels);
    });
  });
};
