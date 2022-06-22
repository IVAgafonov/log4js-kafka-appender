const Kafka = require('kafkajs');
const os = require('os');

let ready = false;

let messages = [];

function loggingEvent2Message(loggingEvent, config) {
    return {
        timestamp: new Date().getTime().toString(),
        value: JSON.stringify({
            _source: config.source + '@' + os.hostname(),
            _message: loggingEvent.data[0],
            _level: loggingEvent.level.levelStr,
            _inputname: 'nodejs_logs',
            _pid: loggingEvent.pid,
            _callStack: loggingEvent.callStack.trim(),
            _category: loggingEvent.categoryName,
            _file: loggingEvent.fileName + ':' + loggingEvent.lineNumber,
            _context: JSON.stringify(loggingEvent.context)
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
