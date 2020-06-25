import restify from "restify";
import dotenv from "dotenv";
import { connect } from "./api/db";

const server = restify.createServer({
  name: "mk-eagle",
});

server.use(restify.plugins.acceptParser(server.acceptable));
server.use(restify.plugins.queryParser());
server.use(restify.plugins.bodyParser());

dotenv.config();

connect();

export default server;
