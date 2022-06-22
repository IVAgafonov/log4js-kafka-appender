const Kafka = require('kafkajs');
const os = require('os');

let ready = false;

let messages = [];

function loggingEvent2Message(loggingEvent, config) {
    return {
        value: JSON.stringify({
            timestamp: Math.round(new Date().getTime() / 1000),
            source: config.source + '@' + os.hostname(),
            message: loggingEvent.data[0],
            level: loggingEvent.level.levelStr,
            inputname: 'nodejs_logs',
            pid: loggingEvent.pid,
            callStack: loggingEvent.callStack.trim(),
            category: loggingEvent.categoryName,
            file: loggingEvent.fileName + ':' + loggingEvent.lineNumber,
            context: JSON.stringify(loggingEvent.context)
        })
    }
}

const kafkaAppender = {
    configure(config) {
        const producer = new Kafka.Kafka({
            clientId: 'log4js-kafka-appender',
            brokers: config.bootstrap
        }).producer();
        producer.connect();

        producer.on('producer.connect', () => {
            ready = true;
            producer.send({
                topic: config.topic,
                messages
            });
            messages = [];
        });

        producer.on('producer.disconnect', () => {
            ready = false;
        });

        return (loggingEvent) => {
            if (ready) {
                producer.send({
                    topic: config.topic,
                    messages: [loggingEvent2Message(loggingEvent, config)]
                })
            } else {
                messages.push(loggingEvent2Message(loggingEvent, config));
            }
        };
    }
}

exports.configure = kafkaAppender.configure;
