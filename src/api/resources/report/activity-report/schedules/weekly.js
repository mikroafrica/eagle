import cron from "cron";

const CronJob = cron.CronJob;
import logger from "../../../../../logger";
import moment from "moment";
import { firstDayOfLastWeek, lastDayOfLastWeek } from "../../../commons/model";
import { SendEmailToKafka, getActivityReport } from "../report.activity-report";

const pastDayInMorning = firstDayOfLastWeek();
const pastDayAtNight = lastDayOfLastWeek();
const schedule = "week";

//Generates user's activity report for the past week and sends to email every Monday of the new weekday by 12:00 AM
export const ActivityReportWeeklyJob = (): CronJob => {
  return new CronJob(
    "0 0 0 * * 1",
    function () {
      const formattedDate = moment.tz("Africa/Lagos");
      logger.info(`::: Weekly Activity report @ ${formattedDate} :::`);

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