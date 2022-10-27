const RdKafka = require('node-rdkafka');
const os = require('os');

let ready = false;

let messages = [];

const hostname = os.hostname();
const projectName = process.env.PROJECT_NAME || '';
const podName = process.env.HOSTNAME || '';
const branch = process.env.BRANCH || '';

function loggingEvent2Message(loggingEvent, config) {
    return Buffer.from(JSON.stringify({
        timestamp: new Date().getTime() / 1000,
        source: config.source + '@' + hostname,
        projectName,
        podName,
        branch,
        message: loggingEvent.data[0],
        level: loggingEvent.level.levelStr,
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
        const producer = new RdKafka.Producer({
            //debug: 'all',
            'client.id': 'customer-report-node',
            'metadata.broker.list': config.bootstrap,
        });

        producer.on('event.log', function(log) {
            switch (log.severity) {
                case 0: console.trace(log.message); break;
                case 7: console.debug(log.message); break;
                case 6: console.info(log.message); break;
                case 4: console.warn(log.message); break;
                case 3: console.error(log.message); break;
                default:
                    console.info(log.message);
            }
        });

        producer.on('ready', () => {
            ready = true;
            messages.forEach(m => producer.produce(config.topic, null, m));
            messages = [];
        });

        producer.on('disconnected', () => {
            ready = false;
        });

        producer.connect();

        return (loggingEvent) => {
            if (ready) {
                producer.produce(config.topic, null, loggingEvent2Message(loggingEvent, config));
            } else {
                messages.push(loggingEvent2Message(loggingEvent, config));
            }
        };
    }
}

exports.configure = kafkaAppender.configure;
