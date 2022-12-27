const os = require('os');
const {Kafka, logLevel} = require("kafkajs");

process.env['KAFKAJS_NO_PARTITIONER_WARNING'] = 1;

const hostname = os.hostname();
const projectName = process.env.PROJECT_NAME || '';
const podName = process.env.HOSTNAME || '';
const branch = process.env.BRANCH || '';

let messages = [];
let connected = false;

function loggingEvent2Message(loggingEvent, config) {
    return Buffer.from(JSON.stringify({
        timestamp: new Date().getTime() / 1000,
        source: config.source + '@' + hostname,
        projectName,
        podName,
        branch,
        message: loggingEvent.data[0],
        level: loggingEvent.level.level,
        levelStr: loggingEvent.level.levelStr,
        inputname: 'nodejs_logs',
        pid: loggingEvent.pid,
        callStack: loggingEvent.callStack?.trim() || loggingEvent.data[1] || '',
        category: loggingEvent.categoryName,
        file: loggingEvent.fileName + ':' + loggingEvent.lineNumber,
        context: JSON.stringify(loggingEvent.context)
    }));
}

const kafkaAppender = {
    configure(config) {
        const producer = new Kafka({
            brokers: config.bootstrap,
            clientId: config.source,
            logCreator: (level) => (logEntry) => {
                switch (level) {
                    case logLevel.DEBUG:
                        return console.debug(logEntry.log.message);
                    case logLevel.ERROR:
                        return console.error(logEntry.log.message);
                    case logLevel.WARN:
                        return console.warn(logEntry.log.message);
                    default:
                        return console.log(logEntry.log.message);
                }
            }
        }).producer({
            retry: {
                initialRetryTime: 10,
                multiplier: 10,
                retries: 10,
                restartOnFailure: async (e) => { console.error(`Kafka failure: ${e.message || e}`); return true; }
            },
            allowAutoTopicCreation: false,
        });

        producer.connect().then(() => {
            connected = true;
            messages.forEach(_ => producer.send({topic: config.topic, messages: [{value: _}]})
                .catch(e => console.error(`Error during processing log: ${e.message || e}`)));
        });

        return (loggingEvent) => {
            if (connected) {
                producer.send({topic: config.topic, messages: [{value: loggingEvent2Message(loggingEvent, config)}]})
                    .catch(e => console.error(`Error during processing log: ${e.message || e}`));
            } else {
                messages.push(loggingEvent2Message(loggingEvent, config));
            }
        };
    }
}

exports.configure = kafkaAppender.configure;
