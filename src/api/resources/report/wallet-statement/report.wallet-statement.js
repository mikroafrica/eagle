import cron from "cron";

const CronJob = cron.CronJob;

import async, { each } from "async";
import Joi from "joi";
import logger from "../../../../logger";
import moment from "moment";
import mongodb from "mongodb";
import { generate_wallet_statement } from "../../services/wallet.service";
import { firstDayOfLastMonth, lastDayOfLastMonth } from "../../commons/model";

const MongoClient = mongodb.MongoClient;

function sendMonthlyWalletStatement(sendMailCallback) {
  MongoClient.connect(
    process.env.CONSUMER_SERVICE_MONGO_URI,
    {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    },
    function (err, db) {
      if (err) {
        throw err;
      }
      const dbo = db.db(process.env.CONSUMER_SERVICE_MONGO_DB_NAME);

      dbo
        .collection("user")
        .find({})
        .toArray(function (err, users) {
          async.forEachOf(
            users,
            async (user, key, callback) => {
              const address = await dbo
                .collection("user-address")
                .findOne({ userId: user._id.toString() });

              const stores = await dbo
                .collection("store")
                .find({ userId: user._id.toString() })
                .toArray();

              sendMailCallback(stores, address, user);
            },
            (err) => {
              logger.info(`Error is ${err}`);
            }
          );
        });
    }
  );
}

export const PreviousMonthWalletStatementReportJob = (): CronJob => {
  return new CronJob(
    "0 0 0 1 * *",
    function () {
      const formattedDate = moment.tz("Africa/Lagos");
      logger.info(`::: Monthly wallet statement @ ${formattedDate} :::`);

      sendMonthlyWalletStatement(function (stores, address, user) {
        sendWalletStatement(stores, address, user);
      });
    },
    undefined,
    true,
    "Africa/Lagos"
  );
};

const firstDayOfLastMonthDate = firstDayOfLastMonth();
const lastDayOfLastMonthDate = lastDayOfLastMonth();

function sendWalletStatement(stores, address, user) {
  for (let store of stores) {
    for (let wallet of store.wallet) {
      let data = {
        walletId: wallet._id,
        dateFrom: firstDayOfLastMonthDate.toString(),
        dateTo: lastDayOfLastMonthDate.toString(),
        type: wallet.type,
        email: user.email,
        address: address.name,
        phoneNumber: user.phoneNumber,
        businessName: user.businessName,
        businessType: user.businessType,
      };

      var schema = Joi.object().keys({
        walletId: Joi.string().required(),
        dateFrom: Joi.string().required(),
        dateTo: Joi.string().required(),
        type: Joi.string(),
        email: Joi.string().required(),
        address: Joi.string().required(),
        phoneNumber: Joi.string().required(),
        businessName: Joi.string().required(),
        businessType: Joi.string().required(),
      });

      const validateSchema = Joi.validate(data, schema);
      if (!validateSchema.error) {
        generate_wallet_statement(data)
          .then((response) => {
            console.log(response);
          })
          .catch((err) => {
            console.log(err);
            logger.error(
              `failed to send monthly report with error ${JSON.stringify(err)}`
            );
          });
      }
    }
  }
}
