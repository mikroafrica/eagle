import restify from "restify-clients";
import { get, post, put } from "../commons/request";

const request = () => {
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
  return post({ client: request, path, params });
};
