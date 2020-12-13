import cron from "cron";

const CronJob = cron.CronJob;
import logger from "../../../../../logger";
import moment from "moment";
import {
  previousDayAtNight,
  previousDayInMorning,
} from "../../../commons/model";
import {
  uploadFileToFileServiceAndSendToKafka,
  getActivityReport,
} from "../report.activity-report";

const pastDayInMorning = previousDayInMorning();
const pastDayAtNight = previousDayAtNight();
const schedule = "week";

export const ActitivityReportWeeklyJob = (): CronJob => {
  return new CronJob(
    // "0 30 2 * * *",
    "*/20 * * * * *",
    function () {
      const formattedDate = moment.tz("Africa/Lagos");
      logger.info(`::: Weekly Activity report @ ${formattedDate} :::`);

      getActivityReport(
        pastDayInMorning,
        pastDayAtNight,
        schedule,
        function (data, email) {
          uploadFileToFileServiceAndSendToKafka(data, email);
        }
      );
    },
    undefined,
    true,
    "Africa/Lagos"
  );
};
