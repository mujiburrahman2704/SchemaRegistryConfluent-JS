const { Kafka, Partitioners } = require("kafkajs")
const { Agent } = require ("https")
const fs = require("fs")
const { SchemaRegistry } = require("@kafkajs/confluent-schema-registry")

console.log('Start')

const agent = new Agent({ 
  keepAlive: true,
  ca: fs.readFileSync('ca.crt'),
  key: fs.readFileSync('schema_registry.key'),
  cert: fs.readFileSync('schema_registry.crt')
})

const registry = new SchemaRegistry({ 
  host: 'https://broker1.muji.com:8081',
  auth: {
    username: 'admin',
    password: 'kafka123'
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
    username: 'admin',
    password: 'kafka123'
  }
})

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })

const run = async() => {
  const i = 10
  let vield = "mujiburrahman"
  for (let index = 0; index < i; index++) {
    await producer.connect()
    const value = 'muji-avro-value'
    // const key = 'muji-avro-key'
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
        my_field3: vield
      })
    }
    await producer.send({
      topic: "muji-avro",
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
