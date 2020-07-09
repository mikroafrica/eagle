import cron from "cron";
const CronJob = cron.CronJob;

import logger from "../../../../logger";
import moment from "moment";
import {
  convertTimeStampToDate,
  previousDayAtNight,
  previousDayInMorning,
} from "../../commons/model";
import { TransactionServiceClient } from "../../../db";
import { fileReport } from "../../services/file.service";
import type { SlackModel } from "../../commons/model";

const mikroKafkaClient = require("mikro-kafka-client");
const mikroProducer = mikroKafkaClient.mikroProducer;
const KafkaConfig = mikroKafkaClient.kafkaConfig;

function computeTerminalReport(callback) {
  const client = TransactionServiceClient();

  const pastDayInMorning = previousDayInMorning();
  const pastDayAtNight = previousDayAtNight();

  // compute all terminal related transactions
  const query = {
    text:
      "SELECT  COUNT(tnx.id), tnx.customer_biller_id, SUM(tnx.amount) AS amount, " +
      "SUM(CASE WHEN status.name = 'successful' THEN tnx.amount else NULL END) AS successfulAmount, " +
      "SUM(CASE WHEN status.name != 'successful' THEN tnx.amount else NULL END) AS failedAmount, " +
      "COUNT(CASE WHEN status.name = 'successful' THEN 1 else NULL END) AS successful , " +
      "COUNT(CASE WHEN status.name != 'successful' THEN 1 else NULL END) AS failed " +
      "FROM public.transactions AS tnx JOIN public.transaction_types AS tnxType ON tnx.transaction_type = tnxType.id " +
      "JOIN public.transaction_statuses status ON  status.id = tnx.transaction_status " +
      "WHERE tnx.time_created >= $1 AND tnx.time_created <= $2 " +
      "AND tnx.transaction_type = 2 GROUP BY tnx.customer_biller_id",

    values: [pastDayInMorning, pastDayAtNight],
  };
  client
    .query(query)
    .then((response) => {
      const results = response.rows;

      if (results.length > 0) {
        const time = convertTimeStampToDate(pastDayAtNight);
        const fileName = `Terminal Report - ${time}`;

        callback(results, fileName, time);
      }
      client.end();
    })
    .catch((error) => {
      logger.error(
        `error occurred while computing terminal report with error [${error}]`
      );
      callback();
    });
}

// run job at every 1:30 A.M
export const PreviousDayTerminalReportJob = (): CronJob => {
  return new CronJob(
    "0 30 1 * * *",
    function () {
      const formattedDate = moment.tz("Africa/Lagos");
      logger.info(`::: Terminal report @ ${formattedDate} :::`);

      computeTerminalReport(function (data, filename, time) {
        pushReportToSlack(data, filename, time);
      });
    },
    undefined,
    true,
    "Africa/Lagos"
  );
};

function pushReportToSlack(report, filename, time) {
  const params = { report, filename, type: "csv" };

  fileReport({ params })
    .then((responseData) => {
      const retentionData = responseData.data;
      const retentionLink = retentionData.data.url;

      const config: KafkaConfig = {
        hostname: process.env.KAFKA_HOST,
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD,
        topic: process.env.KAFKA_SLACK_NOTIFICATION_TOPIC,
      };

      const model: SlackModel = {
        title: `Terminal Report for ${time}`,
        channel: process.env.REPORT_CHANNEL,
        message: "Terminal upload can be found here : `" + retentionLink + "`",
      };
      mikroProducer(config, JSON.stringify(model), function (err, data) {
        if (err) {
          logger.error(
            `error occurred while publishing transaction summary to slack [${err}]`
          );
        }
        logger.info(`published terminal report`);
      });
    })
    .catch((err) => {
      console.log(err);
      logger.error(
        `failed to push terminal report with error ${JSON.stringify(err)}`
      );
    });
}
