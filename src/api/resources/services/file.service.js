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

export const convertToBase64 = (value) => {
  const buffer = Buffer.from(value);
  return buffer.toString("base64");
};

const BasicAuth = () => {
  return convertToBase64(
    `${process.env.MEDIA_SERVICE_USERNAME}:${process.env.MEDIA_SERVICE_PASSWORD}`
  );
};

const uploadFile = async (fileStream, fileFormat)  => {
  const basicAuth = BasicAuth();
  const options = {
    method: "POST",
    uri: `${process.env.MEDIA_SERVICE_URL}/file/upload?format=${fileFormat}`,
    headers: {
      Authorization: `Basic ${basicAuth}`
    },
    formData: {
      media: fileStream
    }
  };
  return request(options);
};

export const uploadMediaFile = async (fileStream, fileFormat) => {
  if (!fileStream) {
    return Promise.reject({
      statusCode: BAD_REQUEST,
      message: `File cannot be empty`
    });
  }

  try {
    const fileData = await uploadFile(fileStream, fileFormat);
    const parsedData = JSON.parse(fileData);
    const id = parsedData.data.id;

    return Promise.resolve({
      statusCode: OK,
      data: { id }
    });
  } catch (e) {
    logger.error(`file upload failed with error ${e}`);
    return Promise.reject({
      statusCode: BAD_REQUEST,
      message: `File upload failed`
    });
  }
}
