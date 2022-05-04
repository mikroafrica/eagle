import cron from "cron";
const CronJob = cron.CronJob;

import moment from "moment";
import logger from "../../../../../logger";
import {
  handleListOfHits,
  ReportIndex,
  search,
} from "../../../../modules/report-service";
import { pastMinutes, previousDayInMorning } from "../../../commons/model";
import { PaymentType } from "../transfer/retry.transfer";
import reQueryTransferEvent, {
  REQUERY_TRANSACTION_EMITTER,
} from "../transfer/requery.transfer.event";
import reQueryBillEvent, {
  RE_QUERY_BILL_EMITTER,
} from "../bills/requery.bills.event";

const TransactionType = {
  Transfer: "TRANSFER",
  Data: "DATA",
  CableTv: "CABLE_TV",
  Phcn: "PHCN",
  Airtime: "AIRTIME",
};

export const TransactionStatus = {
  Pending: "PENDING",
  PaymentSuccessful: "PAYMENT SUCCESSFUL",
  BillPurchasedFailed: "BILL PURCHASED FAILED",
};

const query = ({ startTime, endTime }) => {
  const must = [];

  must.push({
    range: {
      timeCreated: {
        lte: endTime,
      },
    },
  });

  must.push({
    range: {
      timeCreated: {
        gte: startTime,
      },
    },
  });

  must.push({
    bool: {
      should: [
        {
          match: {
            "transactionType.keyword": TransactionType.Phcn,
          },
        },
        {
          match: {
            "transactionType.keyword": TransactionType.Airtime,
          },
        },
        {
          match: {
            "transactionType.keyword": TransactionType.Data,
          },
        },
        {
          match: {
            "transactionType.keyword": TransactionType.CableTv,
          },
        },
        {
          match: {
            "transactionType.keyword": TransactionType.Transfer,
          },
        },
      ],
      minimum_should_match: 1,
    },
  });
  must.push({
    bool: {
      should: [
        {
          match: {
            "transactionStatus.keyword": TransactionStatus.BillPurchasedFailed,
          },
        },
        {
          match: {
            "transactionStatus.keyword": TransactionStatus.Pending,
          },
        },
        {
          match: {
            "transactionStatus.keyword": TransactionStatus.PaymentSuccessful,
          },
        },
      ],
      minimum_should_match: 1,
    },
  });

  const query = {
    index: ReportIndex.TRANSACTION,
    from: 0,
    size: 150,
    _source: [
      "meta",
      "reference",
      "vendor",
      "userId",
      "amount",
      "product",
      "userdata",
      "product",
      "timeUpdated",
      "transactionType",
      "transactionStatus",
      "customerBillerId",
    ],
    body: {
      query: {
        bool: {
          must,
        },
      },
      sort: {
        timeCreated: { order: "desc" },
      },
    },
  };

  logger.info(
    `::: Transaction query request is [${JSON.stringify(query)}] :::`
  );
  return query;
};

const QueryPendingTransactions = async () => {
  try {
    const previousDay = previousDayInMorning();
    const pastThreeMinutes = pastMinutes(2);
    const startTime = new Date(previousDay);
    const endTime = new Date(pastThreeMinutes);
    const queryResponse = await search(query({ startTime, endTime }));
    const { data: queryResponseData } = queryResponse.data;
    const { list: transactionList, total } = handleListOfHits(
      queryResponseData
    );

    logger.info(`::: Total transaction report list is [${total}] :::`);

    const paymentDtoList = [];
    const billingDtoList = [];

    for (let i = 0; i < transactionList.length; i++) {
      const transaction = transactionList[i];

      const amount = parseFloat(transaction.amount);

      if (transaction.transactionType === TransactionType.Transfer) {
        const paymentDto = {
          userId: transaction.userId,
          amount: amount,
          bankCode: transaction.product,
          customerName: transaction.userdata.name,
          remarks: transaction.userdata.remarks,
          transactionRef: transaction.reference,
          accountNumber: transaction.customerBillerId,
          type: PaymentType.BANK_TRANSFER_REQUERY,
          paymentDate: new Date(transaction.timeUpdated).getTime(),
          terminalId: transaction.userdata?.meta?.terminalId,
          serialNumber: transaction.userdata?.meta?.serialNumber,
          vendor: transaction?.userdata?.meta?.dedicatedVendorName,
        };
        paymentDtoList.push(paymentDto);
      }

      if (
        transaction.transactionType === TransactionType.Phcn ||
        transaction.transactionType === TransactionType.Data ||
        transaction.transactionType === TransactionType.CableTv ||
        transaction.transactionType === TransactionType.Airtime
      ) {
        const billingDto = {
          transactionReference: transaction.reference,
          vendor: transaction.vendor
            ? transaction.vendor.toLocaleLowerCase()
            : "",
          phoneNumber:
            transaction.meta.data.customerPhoneNumber ||
            transaction.customerBillerId,
          amount: amount,
          productId: transaction.meta.data.productId,
          meterNumber: transaction.customerBillerId,
          type: transaction.transactionType.toLocaleLowerCase(),
          smartCardNumber: transaction.customerBillerId,
          category: transaction.transactionType.toLocaleLowerCase(),
        };
        billingDtoList.push(billingDto);
      }
    }

    if (paymentDtoList.length > 0) {
      reQueryTransferEvent.emit(REQUERY_TRANSACTION_EMITTER, paymentDtoList);
    }

    if (billingDtoList.length > 0) {
      reQueryBillEvent.emit(RE_QUERY_BILL_EMITTER, billingDtoList);
    }
  } catch (err) {
    console.error(err);
    logger.error(
      `::: failed to fetch report transactions with error [${JSON.stringify(
        err
      )}] :::`
    );
  }
};

// run job every one minutes
export const RetryReportJob = (): CronJob => {
  return new CronJob("0 */3 * * * *", function () {
    const formattedDate = moment.tz("Africa/Lagos");
    logger.info(
      `::: reQuery for pending transaction reports started ${formattedDate} :::`
    );

    QueryPendingTransactions()
      .then((paymentDtoList) => {
        // reQueryTransferEvent.emit(REQUERY_TRANSACTION_EMITTER, paymentDtoList);
      })
      .catch((err) => {
        logger.error(
          `error occurred while publishing transfer result: ${err} `
        );
      });
  });
};
