import { SchemaType, SchemaHelper, ConfluentSchema, LegacyOptions, ProtocolOptions, Schema, AvroSchema } from './@types';
export declare const schemaTypeFromString: (schemaTypeString: string) => SchemaType;
export declare const helperTypeFromSchemaType: (schemaType?: SchemaType) => SchemaHelper;
export declare const schemaFromConfluentSchema: (confluentSchema: ConfluentSchema, options?: ProtocolOptions | LegacyOptions | undefined) => Schema | AvroSchema;
