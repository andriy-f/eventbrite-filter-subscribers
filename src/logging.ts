import pino from 'pino';

// const logLevels = ['trace', 'debug', 'info', 'warn', 'error', 'fatal'] as const;

// const isLogLevel = (value: any): value is typeof logLevels[number] => {
//     return logLevels.includes(value);
// };

export const logger = pino({
    // level: process.env.LOG_LEVEL && isLogLevel(process.env.LOG_LEVEL) ? process.env.LOG_LEVEL : 'debug',
    level: process.env.LOG_LEVEL ?? 'info',
    // formatters: {
    //     level(label) {
    //         return { level: label };
    //     },
    // },
});
