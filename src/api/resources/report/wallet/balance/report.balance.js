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
      "SELECT SUM(wallet.balance) AS balance FROM wallets wallet WHERE wallet.wallet_type_id = $1 ",
    values: [1],
  };

  const client = WalletServiceClient();

  client
    .query(query)
    .then((response) => {
      const results = response.rows;

      logger.info(`The result Wallet balance is [${JSON.stringify(results)}]`);

      if (results.length > 0) {
        const walletBalanceObject = results[0];
        const walletBalance = walletBalanceObject.balance;
        const roundBalance = parseFloat(walletBalance).toFixed(2);
        const formatter = new Intl.NumberFormat("de-DE", {
          style: "currency",
          currency: "NGN",
        });

        callback(formatter.format(roundBalance));
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

      sumUpWalletBalance(function (walletBalance) {
        pushWalletReportToSlack(walletBalance);
      });
    },
    undefined,
    true,
    "Africa/Lagos"
  );
};

function pushWalletReportToSlack(walletBalance) {
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
    message: "`Total Wallet Balance: `" + walletBalance,
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
