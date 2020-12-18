import logger from "./logger";
import server from "./server";
import { RetryTransferJob } from "./api/resources/transaction-service/requery/transfer";
import { RetryWalletTopUpJob } from "./api/resources/transaction-service/requery/walletop-up";
import { RetryWithdrawalJob } from "./api/resources/transaction-service/requery/withdrawal/requery.withdrawal";
import { QueryPastHourTransactionJob } from "./api/resources/report/transaction/hourly/report.service";
import { PreviousDayRetentionReportJob } from "./api/resources/report/retention";
import { PreviousDayTerminalReportJob } from "./api/resources/report/terminal";
import {
  RetryPaymentTerminalJob,
  RetryPaymentWalletTopUpJob,
} from "./api/resources/payment-service/requery.payment";
import { RetryBillsJob } from "./api/resources/transaction-service/requery/bills";
import { PreviousDayTargetReportJob } from "./api/resources/report/target";
import { PreviousMonthWalletStatementReportJob } from "./api/resources/report/wallet-statement";
import { TagAgentBasedOnGoalStatusJob } from "./api/resources/report/acquisition-goal";
import { ReQueryWalletBalance } from "./api/resources/report/wallet/balance";

// handle all uncaught errors
process.on("uncaughtException", function (err) {
  logger.error(`uncaught error has been fired with Error: ${err}`);
});

RetryTransferJob().start();
RetryWalletTopUpJob().start();
RetryWithdrawalJob().start();
QueryPastHourTransactionJob().start();
PreviousDayRetentionReportJob().start();
PreviousDayTerminalReportJob().start();

RetryPaymentWalletTopUpJob().start();
RetryPaymentTerminalJob().start();
RetryBillsJob().start();

PreviousDayTargetReportJob().start();

ReQueryWalletBalance().start();

// PreviousMonthWalletStatementReportJob().start();
// TagAgentBasedOnGoalStatusJob().start();

const port = process.env.PORT || 9000;
server.listen(port, function () {
  logger.info(`App running @ port ${port}`);
});
