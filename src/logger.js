import { createLogger, format, transports } from 'winston';
const { cli } = format;

const logger = createLogger({
  level: 'info',
  format: cli(),
  transports: [new transports.Console()],
});

export default logger;
