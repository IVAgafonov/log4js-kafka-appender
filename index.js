const os = require('os');
const {Kafka, logLevel} = require("kafkajs");

process.env['KAFKAJS_NO_PARTITIONER_WARNING'] = 1;

const hostname = os.hostname();
const projectName = process.env.PROJECT_NAME || '';
const podName = process.env.HOSTNAME || '';
const branch = process.env.BRANCH || '';

let delayedMessages = [];
let connected = false;

process.on('exit', () => {
    if (delayedMessages.length) {
        console.error("Unprocessed messages left: " + delayedMessages.length)
    }
})

function loggingEvent2Message(loggingEvent, config) {
    return Buffer.from(JSON.stringify({
        timestamp: new Date().getTime() / 1000,
        source: config.source + '@' + hostname,
        projectName,
        podName,
        branch,
        message: loggingEvent.data[0],
        level: loggingEvent.level.level,
        severity: loggingEvent.level.levelStr,
        inputname: 'nodejs_logs',
        pid: loggingEvent.pid,
        callStack: loggingEvent.callStack?.trim() || loggingEvent.data[1] || '',
        category: loggingEvent.categoryName,
        file: loggingEvent.fileName ? loggingEvent.fileName + ':' + loggingEvent.lineNumber : 'unknown',
        context: JSON.stringify(loggingEvent.context)
    }));
}

const kafkaAppender = {
    configure(config) {
        const producer = new Kafka({
            brokers: config.bootstrap,
            clientId: config.source,
            retry: {
                initialRetryTime: 20,
                multiplier: 100,
                retries: 100,
                restartOnFailure: async (e) => { console.error(`Kafka failure: ${e.message || e}`); return true; }
            },
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
                initialRetryTime: 20,
                multiplier: 100,
                retries: 100,
                restartOnFailure: async (e) => { console.error(`Kafka failure: ${e.message || e}`); return true; }
            },
            allowAutoTopicCreation: false,
        });

        producer.connect().then(() => {
            connected = true;
        });

        producer.on('producer.connect', () => {
            connected = true;
        })

        return (loggingEvent) => {
            let msg = {value: loggingEvent2Message(loggingEvent, config), timestamp: new Date().getTime().toString()}
            let messagesToSend = [msg];
            if (connected) {
                if (delayedMessages.length) {
                    messagesToSend = delayedMessages.concat(messagesToSend);
                    delayedMessages = [];
                }
                messagesToSend.forEach(_ => producer.send({topic: config.topic, messages: [_]})
                    //.then(r => console.log("message sent: " + JSON.parse(_.value.toString()).message))
                    .catch(e => {
                        delayedMessages.push(_)
                        console.warn("error, buffer size: " + delayedMessages.length + "; push message to buffer: " + (e.message || e))
                    }));
            } else {
                connected = false;
                delayedMessages.push(msg);
                console.warn("disconnected, push message to buffer. buffer size: " + delayedMessages.length)
            }
        };
    }
}

exports.configure = kafkaAppender.configure;
