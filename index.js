import {Kafka} from "kafkajs";

import * as os from "os";

let ready = false;

let messages = [];

export const kafkaAppender = {
    configure(config) {
        const producer =  new Kafka({
            clientId: 'log-producer',
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
