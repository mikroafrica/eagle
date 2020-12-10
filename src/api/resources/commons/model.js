import moment from "moment";

export const phcnTransactionType = "phcn";
export const cableTransactionType = "cable_tv";
export const airtimeTransactionType = "airtime";
export const dataTransactionType = "data";
export const transferTransactionType = "transfer";
export const walletTopUpTransactionType = "wallet_top_up";
export const withdrawalTransactionType = "withdrawal";
export const pendingTransactionStatus = "pending";
export const paymentSuccessfulTransactionStatus = "payment successful";
export const billerPurchaseTransactionStatus = "bill purchased failed";
export const pendingPaymentReversalStatus = "payment reversed pend";

export const morning = () => {
  const fromDate = new Date();
  fromDate.setHours(0, 0, 0, 0);
  return fromDate.getTime();
};

export const night = () => {
  const toDate = new Date();
  toDate.setHours(23, 59, 0, 0);
  return toDate.getTime();
};

export const pastHour = () => {
  const formattedDate = moment.tz("Africa/Lagos");
  return formattedDate.subtract(1, "hours").valueOf();
};

export const now = () => {
  const formattedDate = moment.tz("Africa/Lagos");
  return formattedDate.valueOf();
};

export const convertTimeStampToTime = (timestamp: number) => {
  const formattedDate = moment(timestamp, "x").tz("Africa/Lagos");
  return formattedDate.format("HH:mm");
};

export const convertTimeStampToDate = (timestamp: number) => {
  const formattedDate = moment(timestamp, "x").tz("Africa/Lagos");
  return formattedDate.format("DD-MMMM-YYYY");
};

const previousDay = (time: string) => {
  const formattedDate = moment(time, ["h:mm A"]).tz("Africa/Lagos");
  return formattedDate.subtract(1, "days").valueOf();
};

export const previousDayInMorning = () => {
  return previousDay("12:00 AM");
};

export const previousDayAtNight = () => {
  return previousDay("11:59 PM");
};

export const firstDayOfMonth = () => {
  const firstdate = moment().startOf("month").format("YYYY-MM-DD");
  return moment(`${firstdate} 00:00:00`).tz("Africa/Lagos").format("x");
};

export const firstDayOfLastMonth = () => {
  const firstDay = moment().subtract(1, "months").startOf("month").format("YYYY-MM-DD");
  return moment(`${firstDay} 00:00:00`).tz("Africa/Lagos").format("x");
}

export const lastDayOfLastMonth = () => {
  const lastDay = moment().subtract(1, "months").endOf("month").format("YYYY-MM-DD");
  return moment(`${lastDay} 00:00:00`).tz("Africa/Lagos").format("x");
}

export type PaymentDto = {
  userId: string,
  amount: number,
  remarks: string,
  paymentDate: string,
  transactionRef: string,
  accountNumber: string,
  bankCode: string,
  customerName: string,
  paymentType: string,
};

export type TransactionMessaging = {
  paymentReference: string,
  amount: number,
  accountNumber: string,
  paymentStatus: string,
  email: string,
  vendor: string,
  type: string,
  callbackResponse: string,
  walletId: string,
  userId: string,
  terminalId: string,
  timeCreated: number,
};

export type TransactionMessagingContainer = {
  messaging: TransactionMessaging,
  transactionReference: string,
};

export const TransactionMessagingType = {
  WALLET_TOP_UP: "WALLET_TOP_UP",
  BANK_TRANSFER: "BANK_TRANSFER",
  TERMINAL: "TERMINAL",
};

export const TransactionStatus = {
  SUCCESS: "SUCCESS",
  FAILED: "FAILED",
  PENDING: "PENDING",
  REVERSAL: "REVERSAL",
  FAILED_REVERSAL: "FAILED_REVERSAL",
  UNKNOWN: "UNKOWN",
};

export type SlackModel = {
  title: string,
  channel: string,
  message: string,
};

export type ReQueryModel = {
  vendor: string,
  transactionReference: string,
};

export type BillingModel = {
  transactionReference: string,
  phoneNumber: string,
  amount: number,
  productId: string,
  meterNumber: string,
  type: string,
  smartCardNumber: string,
  category: string,
};
