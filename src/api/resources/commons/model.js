export const transferTransactionType = "transfer";
export const walletTopUpTransactionType = "wallet_top_up";
export const withdrawalTransactionType = "withdrawal";
export const pendingTransactionStatus = "pending";
export const paymentSuccessfulTransactionStatus = "payment successful";
export const billerPurchaseTransactionStatus = "bill purchased failed";

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
  terminalId: string,
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
