const Kafka = require('kafkajs');
const os = require('os');

let ready = false;

let messages = [];

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
                    messages: [
                        {
                            timestamp: new Date().getTime().toString(),
                            value: JSON.stringify({
                                _source: config.source + '-' + os.hostname(),
                                _message: loggingEvent.data[0]
                            })
                        }
                    ]
                })
            } else {
                messages.push({
                    timestamp: new Date().getTime().toString(),
                    value: JSON.stringify({
                        _source: config.source + '-' + os.hostname(),
                        _message: loggingEvent.data[0]
                    })
                });
            }
        };
    }
}

exports.configure = kafkaAppender.configure;
