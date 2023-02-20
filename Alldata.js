const { Kafka, Partitioners } = require("kafkajs")
const { Agent } = require ("https")
const fs = require("fs")
const { SchemaRegistry } = require("@kafkajs/confluent-schema-registry")

console.log('Start')

const agent = new Agent({ 
  keepAlive: true,
 ca: [fs.readFileSync('nodeAlldata/ca.crt', 'utf-8')],
    key: fs.readFileSync('nodeAlldata/kafka_broker.key', 'utf-8'),
    cert: fs.readFileSync('nodeAlldata/kafka_broker.crt', 'utf-8')
})

const registry = new SchemaRegistry({ 
  host: 'https://node1.alldataint.com:8081',
  auth: {
    username: 'admin',
    password: 'password'
  },
  agent
})

const kafka = new Kafka({ 
  clientId: 'my-app',
  brokers: ['node1.alldataint.com:9094','node2.alldataint.com:9094','node3.alldataint.com:9094'],
  ssl: {
    rejectUnauthorized: false,
    ca: [fs.readFileSync('nodeAlldata/ca.crt', 'utf-8')],
    key: fs.readFileSync('nodeAlldata/kafka_broker.key', 'utf-8'),
    cert: fs.readFileSync('nodeAlldata/kafka_broker.crt', 'utf-8')
  },
  sasl: {
    mechanism: 'plain',
    username: 'admin',
    password: 'password'
  }
})

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })

const run = async() => {
  const i = 3
  for (let index = 0; index < i; index++) {
    await producer.connect()
    const value = 'Avro-muji-value'
    // const key = 'Avro-muji-key'
    const idvalue = await registry.getLatestSchemaId(value)
    // const idkey = await registry.getLatestSchemaId(key)
    console.log({idvalue})
    const outmessage = {
      // key: await registry.encode(idkey, {
      //   id: '1'
      // }),
      value: await registry.encode(idvalue, {
        my_field1: 1,
        my_field2: 1.1,
        my_field3: "satu"
      })
    }
    await producer.send({
      topic: "Avro-muji",
      messages: [outmessage]
    })
    console.log("Done")      
  }
}
run().catch(async e => {
  console.error(e)
  producer && producer.disconnect()
  process.exit(1)
})
