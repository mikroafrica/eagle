import cron from "cron";

const CronJob = cron.CronJob;
import moment from "moment";

import logger from "../../../../../logger";
import { TransactionServiceClient } from "../../../../db";

import reQueryTransferEvent from "./requery.transfer.event";
import { REQUERY_TRANSACTION_EMITTER } from "./requery.transfer.event";
import {
  billerPurchaseTransactionStatus,
  morning,
  night,
  pastMinutes,
  paymentSuccessfulTransactionStatus,
  payoutTransactionType,
  pendingTransactionStatus,
  previousDayInMorning,
  transferTransactionType,
} from "../../../commons/model";
import type { PaymentDto } from "../../../commons/model";

export const PaymentType = {
  BANK_TRANSFER_REQUERY: "BANK_TRANSFER_REQUERY",
  BANK_TRANSFER_REPROCESS: "BANK_TRANSFER_REPROCESS",
};

function reQueryPendingTransfer(callback) {
  const pastThreeMinutes = pastMinutes(3);
  // fetch the first fifteen in ascending order
  const query = {
    text:
      "SELECT * FROM transactions tnx " +
      "JOIN transaction_statuses status ON status.id = tnx.transaction_status " +
      "JOIN transaction_types type ON type.id = tnx.transaction_type " +
      "WHERE (status.name = $1 OR status.name = $2 OR status.name = $3) " +
      "AND (type.name = $4 OR type.name = $5) " +
      "AND tnx.time_created >= $6 AND tnx.time_created <= $7 " +
      "ORDER BY tnx.time_created DESC limit 150 ",

    values: [
      pendingTransactionStatus,
      paymentSuccessfulTransactionStatus,
      billerPurchaseTransactionStatus,
      transferTransactionType,
      payoutTransactionType,
      previousDayInMorning(),
      night(),
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
        const amount = parseFloat(data.amount);
        return {
          userId: data.user_id,
          amount: amount,
          bankCode: data.product,
          customerName: data.userdata.name,
          remarks: data.userdata.remarks,
          paymentDate: data.time_updated,
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

// run job every one minutes
export const RetryTransferJob = (): CronJob => {
  return new CronJob("0 */1 * * * *", function () {
    const formattedDate = moment.tz("Africa/Lagos");
    logger.info(`::: reQuery for transfer started ${formattedDate} :::`);

    reQueryPendingTransfer(function (paymentDtoList: PaymentDto[]) {
      reQueryTransferEvent.emit(REQUERY_TRANSACTION_EMITTER, paymentDtoList);
    });
  });
};
