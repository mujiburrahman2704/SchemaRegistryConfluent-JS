const { Kafka } = require('kafkajs')
const fs = require('fs')

const kafka = new Kafka({ 
  clientId: 'my-app',
  brokers: ['broker1.muji.com:9093','broker2.muji.com:9093','broker3.muji.com:9093'],
  ssl: {
    rejectUnauthorized: false,
    ca: [fs.readFileSync('ca.crt', 'utf-8')],
    key: fs.readFileSync('kafka_broker.key', 'utf-8'),
    cert: fs.readFileSync('kafka_broker.crt', 'utf-8')
  },
  sasl: {
    mechanism: 'plain',
    username: 'admin',
    password: 'kafka123'
  }
})
const consumer = kafka.consumer({ 
  groupId: 'test-group'
})

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic: 'replication-01.test.collection' })

  await consumer.run({
    eachMessage: async ({ topic, pratition, message }) => {
      console.log({
        value: message.value.toString(),
        })
    },
  })
}

run().catch(console.error)