import cron from "cron";

const CronJob = cron.CronJob;
import logger from "../../../../../logger";
import moment from "moment";
import {
  firstDayOfLastMonth,
  lastDayOfLastMonth,
} from "../../../commons/model";
import { SendEmailToKafka, getActivityReport } from "../report.activity-report";

const pastDayInMorning = firstDayOfLastMonth();
const pastDayAtNight = lastDayOfLastMonth();
const schedule = "month";

export const ActitivityReportMonthlyJob = (): CronJob => {
  return new CronJob(
    "0 0 0 1 * *",
    function () {
      const formattedDate = moment.tz("Africa/Lagos");
      logger.info(`::: Monthly Activity report @ ${formattedDate} :::`);

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
