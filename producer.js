const { Kafka } = require('kafkajs')
const fs = require('fs')
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry')
const { Partitioners } = require('kafkajs')

const kafka = new Kafka({ 
  clientId: 'my-app',
  brokers: ['broker1.muji.com:9093','broker2.muji.com:9093','broker3.muji.com:9093'],
  ssl: {
    rejectUnauthorized: false,
    ca: [fs.readFileSync('cacert.pem', 'utf-8')],
    key: fs.readFileSync('broker1.muji.com.pem', 'utf-8'),
    cert: fs.readFileSync('broker1.muji.com.crt', 'utf-8')
  },
  sasl: {
    mechanism: 'plain',
    username: 'admin',
    password: 'admin-secret'
  }
})
const registry = new SchemaRegistry({ 
  host: ['https://broker1.muji.com:8081','https://broker2.muji.com:8081','https://broker3.muji.com:8081']
})

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })
const run = async () => {
  await producer.connect()
  await producer.send({
    topic: 'test-topic',
    messages: [
      { value: 'Hello' },
    ],
  })
  await producer.disconnect()
}
run().catch(console.error)

