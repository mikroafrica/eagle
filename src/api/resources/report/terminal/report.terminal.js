import cron from "cron";

const CronJob = cron.CronJob;

import logger from "../../../../logger";
import moment from "moment";
import {
  convertTimeStampToDate,
  previousDayAtNight,
  previousDayInMorning,
} from "../../commons/model";
import { PaymentServiceClient, TransactionServiceClient } from "../../../db";
import { fileReport } from "../../services/file.service";
import type { SlackModel } from "../../commons/model";

const mikroKafkaClient = require("mikro-kafka-client");
const mikroProducer = mikroKafkaClient.mikroProducer;
const KafkaConfig = mikroKafkaClient.kafkaConfig;

function computeTerminalReport(callback) {
  const client = PaymentServiceClient();

  const pastDayInMorning = previousDayInMorning();
  const pastDayAtNight = previousDayAtNight();

  // compute all terminal related transactions
  const query = {
    text:
      " SELECT  COUNT(tnx.id), profile.phone_number, profile.name, SUM(tnx.amount) AS amount," +
      " tnx.callback_response -> 'callback_response' ->> 'terminalID' as terminalID," +
      "SUM(CASE WHEN tnx.status = 'SUCCESS' THEN tnx.amount else NULL END) AS successfulAmount," +
      "SUM(CASE WHEN tnx.status != 'SUCCESS' THEN tnx.amount else NULL END) AS failedAmount," +
      "COUNT(CASE WHEN tnx.status = 'SUCCESS' THEN 1 else NULL END) AS successful ," +
      "COUNT(CASE WHEN tnx.status != 'SUCCESS' THEN 1 else NULL END) AS failed " +
      "FROM public.transactions AS tnx JOIN public.terminals profile ON  profile.terminal_id = tnx.callback_response -> 'callback_response' ->> 'terminalID' " +
      "WHERE tnx.type = 'TERMINAL' AND tnx.time_created >= $1 AND tnx.time_created < $2" +
      " GROUP BY terminalID, profile.phone_number, profile.name",

    values: [pastDayInMorning, pastDayAtNight],
  };
  client
    .query(query)
    .then((response) => {
      const results = response.rows;

      if (results.length > 0) {
        const time = convertTimeStampToDate(pastDayInMorning);
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

// run job at every 1:45 A.M
export const PreviousDayTerminalReportJob = (): CronJob => {
  return new CronJob(
    "0 45 1 * * *",
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
