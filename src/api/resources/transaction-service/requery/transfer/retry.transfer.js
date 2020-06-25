import cron from "cron";
const CronJob = cron.CronJob;

import moment from "moment";

import logger from "../../../../../logger";
import { TransactionServiceClient } from "../../../../db";

import reQueryTransferEvent from "./requery.transfer.event";
import { REQUERY_TRANSACTION_EMITTER } from "./requery.transfer.event";

const transactionType = "transfer";
const pendingTransactionStatus = "pending";
const paymentSuccessfulTransactionStatus = "payment successful";
const billerPurchaseTransactionStatus = "bill purchased failed";

export const PaymentType = {
  BANK_TRANSFER_REQUERY: "BANK_TRANSFER_REQUERY",
  BANK_TRANSFER_REPROCESS: "BANK_TRANSFER_REPROCESS",
};

export type PaymentDto = {
  userId: string,
  amount: number,
  remarks: string,
  paymentDate: string,
  transactionRef: string,
  accountNumber: string,
  bankCode: string,
  customerName: string,
  paymentType: string,
};

function reQueryPendingTransfer(callback) {
  const fromDate = new Date();
  fromDate.setHours(0, 0, 0, 0);

  const toDate = new Date();
  toDate.setHours(23, 59, 0, 0);

  const query = {
    text:
      "SELECT * FROM transactions tnx " +
      "JOIN transaction_statuses status ON status.id = tnx.transaction_status " +
      "JOIN transaction_types type ON type.id = tnx.transaction_type " +
      "WHERE status.name = $1 OR status.name = $2 OR status.name = $3 " +
      "AND type.name = $4 " +
      "AND tnx.time_created >= $5 AND tnx.time_created <= $6 ",

    values: [
      pendingTransactionStatus,
      paymentSuccessfulTransactionStatus,
      billerPurchaseTransactionStatus,
      transactionType,
      fromDate.getTime(),
      toDate.getTime(),
    ],
  };

  const client = TransactionServiceClient();

  client
    .query(query)
    .then((response) => {
      const results = response.rows;
      logger.info(
        `Total number of queried transfer results is [${results.length}]`
      );
      const paymentListDto: PaymentDto[] = results.map(function (data) {
        const serviceFee = data.meta.data.serviceFee;
        const amount = parseInt(data.amount) - parseInt(serviceFee);
        return {
          userId: data.user_id,
          amount: amount,
          remarks: data.userdata.remarks,
          paymentDate: data.time_created,
          transactionRef: data.transaction_reference,
          accountNumber: data.customer_biller_id,
          paymentType: PaymentType.BANK_TRANSFER_REQUERY,
        };
      });
      callback(paymentListDto);

      client.end();
    })
    .catch((error) => {
      logger.error(
        `error occurred while fetching pending with error [${error}]`
      );
    });
}

export const RetryTransferJob = (): CronJob => {
  return new CronJob("0 */1 * * * *", function () {
    const formattedDate = moment.tz("Africa/Lagos");
    logger.info(`::: reQuery for transfer started ${formattedDate} :::`);

    reQueryPendingTransfer(function (paymentDtoList: PaymentDto[]) {
      reQueryTransferEvent.emit(REQUERY_TRANSACTION_EMITTER, paymentDtoList);
    });
  });
};
