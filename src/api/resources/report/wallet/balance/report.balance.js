import cron from "cron";

const CronJob = cron.CronJob;

const mikroKafkaClient = require("mikro-kafka-client");
const mikroProducer = mikroKafkaClient.mikroProducer;
const KafkaConfig = mikroKafkaClient.kafkaConfig;

import moment from "moment";
import logger from "../../../../../logger";

import type { SlackModel } from "../../../commons/model";
import { WalletServiceClient } from "../../../../db";

function sumUpWalletBalance(callback) {
  const query = {
    text:
      "SELECT ROUND(SUM(wallet.balance), 2) AS balance, " +
      "COUNT(CASE when wallet.wallet_type_id = 1 AND wallet.balance " +
      "BETWEEN 0 AND 100000 then 1 ELSE NULL END) AS belowHundred, " +
      "COUNT(CASE WHEN wallet.wallet_type_id = 1 AND wallet.balance " +
      "BETWEEN 1000001 AND 300000 THEN 1 ELSE NULL END) as belowThreeHundred, " +
      "COUNT(CASE WHEN wallet.wallet_type_id = 1 AND wallet.balance " +
      "BETWEEN 3000001 AND 500000 THEN 1 ELSE NULL END) as belowFiveHundred, " +
      "COUNT(CASE WHEN wallet.wallet_type_id = 1 AND wallet.balance > 500000 THEN 1 ELSE NULL END) AS aboveFiveHundred " +
      "FROM wallets wallet WHERE wallet.wallet_type_id = 1 ",
    values: [],
  };

  const client = WalletServiceClient();

  client
    .query(query)
    .then((response) => {
      const results = response.rows;

      logger.info(`The result Wallet balance is [${JSON.stringify(results)}]`);

      if (results.length > 0) {
        const walletBalance = results[0].balance;
        const belowHundredCount = results[0].belowhundred;
        const belowThreeHundredCount = results[0].belowthreehundred;
        const belowFiveHundredCount = results[0].belowfivehundred;
        const aboveFiveHundredCount = results[0].abovefivehundred;

        const ROUNDBalance = parseFloat(walletBalance).toFixed(2);
        const formatter = new Intl.NumberFormat("de-DE", {
          style: "currency",
          currency: "NGN",
        });

        callback(
          formatter.format(ROUNDBalance),
          belowHundredCount,
          belowThreeHundredCount,
          belowFiveHundredCount,
          aboveFiveHundredCount
        );
      }

      client.end();
    })
    .catch((error) => {
      logger.error(
        `error occurred while fetching wallet balance with error [${error}]`
      );
    });
}

// run job at every 3:30 A.M
export const ReQueryWalletBalance = (): CronJob => {
  return new CronJob(
    "0 30 3 * * *",
    function () {
      const formattedDate = moment.tz("Africa/Lagos");
      logger.info(
        `::: reQuery for wallet balance started ${formattedDate} :::`
      );

      sumUpWalletBalance(function (
        walletBalance,
        belowHundredCount,
        belowThreeHundredCount,
        belowFiveHundredCount,
        aboveFiveHundredCount
      ) {
        pushWalletReportToSlack(
          walletBalance,
          belowHundredCount,
          belowThreeHundredCount,
          belowFiveHundredCount,
          aboveFiveHundredCount
        );
      });
    },
    undefined,
    true,
    "Africa/Lagos"
  );
};

function pushWalletReportToSlack(
  walletBalance,
  belowHundredCount,
  belowThreeHundredCount,
  belowFiveHundredCount,
  aboveFiveHundredCount
) {
  const config: KafkaConfig = {
    hostname: process.env.KAFKA_HOST,
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
    topic: process.env.KAFKA_SLACK_NOTIFICATION_TOPIC,
  };

  const formattedDate = moment.tz("Africa/Lagos");

  const model: SlackModel = {
    title: `Wallet Balance ReQuery @ [${formattedDate}]`,
    channel: process.env.WALLET_CHANNEL,
    message:
      "`Total Wallet Balance: `" +
      walletBalance +
      "\n`Number of Wallet Balance btw 0 and 100k: `" +
      belowHundredCount +
      "\n`Number of Wallet Balance btw 101k and 300k: `" +
      belowThreeHundredCount +
      "\n`Number of Wallet Balance btw 301k and 500k: `" +
      belowFiveHundredCount +
      "\n`Number of Wallet Balance above 500k: `" +
      aboveFiveHundredCount,
  };
  mikroProducer(config, JSON.stringify(model), function (err, data) {
    if (err) {
      logger.error(
        `error occurred while publishing wallet balance to slack [${err}]`
      );
    }
    logger.info(`published wallet balance report`);
  });
}
