import restify from "restify-clients";
import request from "request-promise";
import { OK } from "../../modules/status";
import { post } from "../commons/request";
import logger from "../../../logger";

const requestClient = () => {
  const client = restify.createJSONClient({
    url: process.env.MEDIA_SERVICE_URL,
    version: "*",
  });

  client.basicAuth(
    process.env.MEDIA_SERVICE_USERNAME,
    process.env.MEDIA_SERVICE_PASSWORD
  );
  return client;
};

export const fileReport = ({ params }) => {
  const path = {
    path: "/file",
  };
  return post({ client: requestClient, path, params });
};
