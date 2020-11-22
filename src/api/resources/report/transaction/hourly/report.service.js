import cron from "cron";

const CronJob = cron.CronJob;

import { convertTimeStampToTime, now, pastHour } from "../../../commons/model";
import { TransactionServiceClient } from "../../../../db";
import logger from "../../../../../logger";
import moment from "moment";

import transactionSummaryEvent, {
  TRANSACTION_SUMMARY_EMITTER,
} from "./report.event";

function queryPastHourTransactionByName(callback) {
  const oneHourAgo = pastHour();
  const currentTime = now();

  const query = {
    text:
      "SELECT  COUNT(tnx.id), tnxType.name, SUM(tnx.amount) AS amount," +
      "SUM(CASE WHEN status.name = 'successful' THEN tnx.amount else 0 END) AS successfulAmount, " +
      "SUM(CASE WHEN status.name = 'payment successful' " +
      "or status.name = 'bill purchased failed' or status.name = 'payment pending'" +
      " or status.name = 'pending' THEN tnx.amount else 0 END) AS pendingAmount, " +
      "SUM(CASE WHEN status.name = 'payment failed' THEN tnx.amount else 0 END) AS failedAmount, " +
      "COUNT(CASE WHEN status.name = 'successful' THEN 1 else NULL END) AS successful, " +
      "COUNT(CASE WHEN status.name = 'payment failed' THEN 1 else NULL END) AS failed, " +
      "COUNT(CASE WHEN status.name = 'payment successful' " +
      "or status.name = 'bill purchased failed' or status.name = 'payment pending'" +
      " or status.name = 'pending' THEN 1 else NULL END) AS pending " +
      "FROM public.transactions AS tnx JOIN public.transaction_types AS tnxType ON tnx.transaction_type = tnxType.id " +
      "JOIN public.transaction_statuses status ON  status.id = tnx.transaction_status " +
      "WHERE tnx.time_created >= $1 AND tnx.time_created <= $2 GROUP BY tnxType.name",

    values: [oneHourAgo, currentTime],
  };

  const friendlyTime = `${convertTimeStampToTime(
    oneHourAgo
  )} -- ${convertTimeStampToTime(currentTime)}`;

  const client = TransactionServiceClient();

  client
    .query(query)
    .then((response) => {
      const results = response.rows;
      logger.info(
        `Total number of queried transaction summary for the past hour is [${results.length}]`
      );

      let modifiedValue = "";

      const formatter = new Intl.NumberFormat("de-DE", {
        style: "currency",
        currency: "NGN",
      });

      for (let i = 0; i < results.length; i++) {
        const data = results[i];
        modifiedValue +=
          "Transaction Type: `" +
          data.name +
          "`\n" +
          "Total Transaction Count: `" +
          data.count +
          "`\n" +
          "Total Amount Processed: `" +
          formatter.format(data.amount) +
          "`\n" +
          "Total Successful Amount: `" +
          formatter.format(data.successfulamount) +
          "`\n" +
          "Total Pending Amount: `" +
          formatter.format(data.pendingamount) +
          "`\n" +
          "Total Failed Amount: `" +
          formatter.format(data.failedamount) +
          "`\n" +
          "Successful Count: `" +
          data.successful +
          "`\n" +
          "Pending Count: `" +
          data.pending +
          "`\n" +
          "Failed Count: `" +
          data.failed +
          "`\n\n";
      }

      if (modifiedValue.length !== 0) {
        callback(modifiedValue, friendlyTime);
      }

      client.end();
    })
    .catch((error) => {
      logger.error(
        `error occurred while fetching past hour transaction with error [${error}]`
      );
    });
}

export const QueryPastHourTransactionJob = (): CronJob => {
  return new CronJob("0 0 */1 * * *", function () {
    const formattedDate = moment.tz("Africa/Lagos");
    logger.info(`::: Query past hour transaction started ${formattedDate} :::`);

    queryPastHourTransactionByName(function (data, time) {
      transactionSummaryEvent.emit(TRANSACTION_SUMMARY_EMITTER, data, time);
    });
  });
};
