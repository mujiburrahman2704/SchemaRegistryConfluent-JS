const { Kafka } = require('kafkajs')
const { Agent } = require("https")
const path = require('path')
const fs = require('fs')
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry')

const agent = new Agent({ 
  keepAlive: true,
  ca: fs.readFileSync('ca.crt'),
  key: fs.readFileSync('schema_registry.key'),
  cert: fs.readFileSync('schema_registry.crt')
})

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
    username: 'c3',
    password: 'c31'
  }
})

const registry = new SchemaRegistry({
  host: 'https://broker1.muji.com:8081',
  auth: {
    username: "c3",
    password: "c31"
  },
  agent
})

const consumer = kafka.consumer({groupId: 'test-group1'})
const incomingtopic = 'muji-avro'

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({
    topic: incomingtopic,
    fromBeginning: true
  })
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const decodedvalue = await registry.decode(message.value)
      // const decodedkey = await registry.decode(message.key)
      console.log(decodedvalue)
    }
  })
}
run().catch(console.error)