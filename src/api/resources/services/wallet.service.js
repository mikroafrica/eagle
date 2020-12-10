import restify from "restify-clients";
import { get } from "../commons/request";

const request = () => {
  const client = restify.createJSONClient({
    url: process.env.WALLET_SERVICE_URL,
    version: "*",
  });

  client.basicAuth(
    process.env.WALLET_USERNAME,
    process.env.WALLET_PASSWORD
  );
  return client;
};

export const generate_wallet_statement = ({ 
  walletId,
  dateFrom,
  dateTo,
  type,
  email,
  address,
  phoneNumber,
  businessName,
  businessType, 
}) => {
  const path = {
    path: `/transactions/${walletId}/generate-statement`,
    query: {
      dateFrom,
      dateTo,
      type,
      email,
      address,
      phoneNumber,
      businessName,
      businessType,
    },
  };
  return get({ client: request, path });
};
