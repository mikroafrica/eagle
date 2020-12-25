import cron from "cron";

const CronJob = cron.CronJob;
import logger from "../../../../../logger";
import moment from "moment";
import {
  previousDayAtNight,
  previousDayInMorning,
} from "../../../commons/model";
import { SendEmailToKafka, getActivityReport } from "../report.activity-report";

const pastDayInMorning = previousDayInMorning();
const pastDayAtNight = previousDayAtNight();
const schedule = "day";

//Generates user's activity report for the past day and sends to email every day by 5:00 AM
export const ActivityReportDailyJob = (): CronJob => {
  return new CronJob(
    "0 0 5 * * *",
    function () {
      const formattedDate = moment.tz("Africa/Lagos");
      logger.info(`::: Daily Activity report @ ${formattedDate} :::`);

      getActivityReport(
        pastDayInMorning,
        pastDayAtNight,
        schedule,
        function (data, email) {
          SendEmailToKafka(data, email);
        }
      );
    },
    undefined,
    true,
    "Africa/Lagos"
  );
};
