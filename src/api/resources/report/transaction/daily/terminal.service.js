import cron from "cron";

const CronJob = cron.CronJob;

import {
  convertTimeStampToDate,
  convertTimeStampToTime,
  previousDayAtNight,
  previousDayInMorning,
  yesterday,
} from "../../../commons/model";
import { PaymentServiceClient, TransactionServiceClient } from "../../../../db";
import logger from "../../../../../logger";
import moment from "moment";

import terminalTransactionSummaryEvent, {
  TERMINAL_EMITTER,
} from "./terminal.event";

function queryTerminalTransaction(callback) {
  const previousMorning = previousDayInMorning();
  const previousNight = previousDayAtNight();

  const query = {
    text:
      "select sum(case when tnx.status = 'SUCCESS' THEN tnx.amount ELSE 0 END) as success, " +
      "sum(case when tnx.status = 'FAILED' THEN tnx.amount ELSE 0 END) as failed, " +
      "sum(case when tnx.status = 'REVERSAL' THEN tnx.amount ELSE 0 END) as reversal, " +
      "sum(case when tnx.status = 'FAILED_REVERSAL' THEN tnx.amount ELSE 0 END) as failedRev, " +
      " term.vendor, term.bank from transactions tnx " +
      "join terminals term  on term.terminal_id = tnx.customer_biller_id " +
      "where type = 'TERMINAL' and tnx.time_created <= $1 and tnx.time_created < $2  group by term.vendor, term.bank",

    values: [previousMorning, previousNight],
  };

  console.log(previousMorning);
  console.log(previousNight);

  const friendlyTime = `${convertTimeStampToDate(
    previousMorning
  )} ${convertTimeStampToTime(previousMorning)} -- ${convertTimeStampToDate(
    previousNight
  )} ${convertTimeStampToTime(previousNight)}`;

  console.log(friendlyTime);

  const paymentServiceClient = PaymentServiceClient();

  paymentServiceClient
    .query(query)
    .then((response) => {
      const results = response.rows;
      logger.info(
        `Total number of queried terminal summary is [${results.length}]`
      );

      let modifiedValue = "";

      results.forEach(function (result) {
        modifiedValue +=
          "`Transaction for PTSP (" +
          result.vendor +
          ") " +
          "and Bank (" +
          result.bank +
          ")`" +
          " ```success: " +
          result.success +
          " \n" +
          "Failed: " +
          result.failed +
          " \n" +
          "Reversal: " +
          result.reversal +
          " \n" +
          "Failed Reversal: " +
          result.failedrev +
          " ``` ";
      });

      const transactionQuery = {
        text:
          "select sum(case when tnx.transaction_status = 6 then tnx.amount else 0 end) as success " +
          "from transactions tnx where tnx.time_created between $1 and $2 and transaction_type = 2",

        values: [previousMorning, previousNight],
      };

      const transactionServiceClient = TransactionServiceClient();
      transactionServiceClient.query(transactionQuery).then((response) => {
        const tnxResult = response.rows;
        logger.info(
          `Total number of queried transaction summary is [${results.length}]`
        );

        if (tnxResult.length > 0) {
          const success = tnxResult[0].success;

          modifiedValue += "`Wallet Credit` ``` " + success + " ```";
        }

        callback(modifiedValue, friendlyTime);

        paymentServiceClient.end();

        transactionServiceClient.end();
      });
    })
    .catch((error) => {
      logger.error(
        `error occurred while fetching past hour transaction with error [${error}]`
      );
    });
}

// run job every 5: 00 a.m
export const QueryPastDayTerminalTransactionJob = (): CronJob => {
  return new CronJob(
    // "* * * * * *",
    "0 */2 * * * *",
    // "0 0 6 * * *",
    function () {
      const formattedDate = moment.tz("Africa/Lagos");
      logger.info(
        `::: Query terminal transaction started ${formattedDate} :::`
      );

      queryTerminalTransaction(function (data, time) {
        terminalTransactionSummaryEvent.emit(TERMINAL_EMITTER, data, time);
      });
    },
    undefined,
    true,
    "Africa/Lagos"
  );
};
