const { Agent } = require ('https')
const { SchemaRegistry, SchemaType } = require ('@kafkajs/confluent-schema-registry')
const fs = require ('fs')
//const path = require('path')
//const { SchemaRegistry, SchemaType } = require('@kafkajs/confluent-schema-registry')

const agent = new Agent({ 
    keepAlive: true,
    ca: fs.readFileSync('ca.crt'),
    key: fs.readFileSync('schema_registry.key'),
    cert: fs.readFileSync('schema_registry.crt')
 })

const registry = new SchemaRegistry({ 
    host: 'https://broker1.muji.com:8081',
    auth: {
        username: 'c3',
        password: 'c31'
    }, 
    agent
 })

// Upload a schema to the r egistry
const schema = `
  {
    "type": "record",
    "name": "RandomTest",
    "namespace": "examples",
    "fields": [{ "type": "string", "name": "fullName" }]
  }
`
const schemas = {
    type: SchemaType.AVRO,
    schema: schema
  }
const options = {
    subject: "topic-test-value"
  }

const run = async () => {
     await registry.register(schemas,options)
}

run().catch(console.error)
// Encode using the uploaded schema
//const payload = { fullName: 'John Doe' }
//const encodedPayload = await registry.encode(id, payload)

// Decode the payload
//const decodedPayload = await registry.decode(encodedPayload)