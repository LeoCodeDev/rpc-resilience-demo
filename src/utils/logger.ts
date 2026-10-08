export interface ILogger {
  info(msg: string): void;
  warn(msg: string): void;
  error(msg: string): void;
}

const COLORS = { info: "\x1b[36m", warn: "\x1b[33m", error: "\x1b[31m" } as const;
const RESET = "\x1b[0m";

/** Minimal console logger; swap for pino/winston in a real service. */
export const consoleLogger: ILogger = {
  info: (m) => console.log(`${COLORS.info}INFO ${RESET} ${m}`),
  warn: (m) => console.log(`${COLORS.warn}WARN ${RESET} ${m}`),
  error: (m) => console.log(`${COLORS.error}ERROR${RESET} ${m}`),
};
