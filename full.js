const { Kafka, Partitioners } = require("kafkajs")
const { Agent } = require ("https")
const fs = require("fs")
const { SchemaRegistry } = require("@kafkajs/confluent-schema-registry")

const agent = new Agent({ 
    keepAlive: true,
    ca: [fs.readFileSync('ca.crt', 'utf-8')],
    key: fs.readFileSync('kafka_broker.key', 'utf-8'),
    cert: fs.readFileSync('kafka_broker.crt', 'utf-8')
})

const registry = new SchemaRegistry({ 
    host: 'https://broker1.muji.com:8081',
    auth: {
      username: 'c3',
      password: 'c31'
    },
    agent
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

const consumer = kafka.consumer({ groupId: 'test' })
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })
const topic = "topic-test"
const run = async () => {
    const subject = 'topic-test-value'
    const id = await registry.getLatestSchemaId(subject)
    await consumer.connect()
    await producer.connect()
  
    await consumer.subscribe({ topic: topic , fromBeginning: true})
  
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const decodedMessage = {
          ...message,
          value: await registry.decode(message.value)
        }
        console.log("Success This is Data consume = ", decodedMessage.value)
  
        const outgoingMessage = {
          key: message.key,
          value: await registry.encode(id, decodedMessage.value)
        }
  
        await producer.send({
          topic: topic,
          messages: [ outgoingMessage ]
        })
        console.log("Success This is Data consume = ", decodedMessage.value)
      },
    })
  }
  
  run().catch(async e => {
    console.error(e)
    consumer && await consumer.disconnect()
    producer && await producer.disconnect()
    process.exit(1)
  })