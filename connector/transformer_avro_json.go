package connector

import (
	"encoding/binary"
	"errors"
	hambaavro "github.com/hamba/avro/v2"
	"strconv"
	"time"

	ttlcache "github.com/myrteametrics/myrtea-sdk/v5/cache"
	"github.com/myrteametrics/myrtea-sdk/v5/utils"
)

// AvroToJSONTransformer :
type AvroToJSONTransformer struct {
	schemaRegistryEndpoint string
	client                 *utils.CachedSchemaRegistry
	cache                  *ttlcache.Cache
}

// Transform is the convertor transformer, it decodes the AVRO message into an interface{} message
func (transformer AvroToJSONTransformer) Transform(msg Message, to *any) error {
	switch kafkaMsg := msg.(type) {
	case KafkaMessage:
		schemaStr, schemaID, bytes, err := transformer.getSchemaFromAvroBinary(kafkaMsg.Data)
		if err != nil {
			return err
		}

		schema, err := transformer.getSchema(schemaStr, schemaID)
		if err != nil {
			return err
		}

		return hambaavro.Unmarshal(schema, bytes, to)
	default:
		return errors.New("couldn't transform the Message, the convertor transformer couldn't get the Type of the incoming message")
	}
}

// getSchema parses the schema string and returns the schema object, it also caches the schema
func (transformer AvroToJSONTransformer) getSchema(schemaStr string, schemaID int) (hambaavro.Schema, error) {
	idStr := strconv.Itoa(schemaID)
	value, exists := transformer.cache.Get(idStr)
	if exists {
		return value.(hambaavro.Schema), nil
	}

	schema, err := hambaavro.Parse(schemaStr)
	if err != nil {
		return nil, err
	}

	transformer.cache.Set(schemaStr, schema)
	return schema, nil
}

// NewAvroToJSONTransformer New transformer constructor
// TODO : Manage multiple schemaRegistryEndpoint ? In case of server failure ?
func NewAvroToJSONTransformer(schemaRegistryEndpoint string, ttlCacheDuration time.Duration) (*AvroToJSONTransformer, error) {
	client, err := utils.NewCachedSchemaRegistry(schemaRegistryEndpoint, ttlCacheDuration)
	if err != nil {
		return nil, err
	}
	cache := ttlcache.NewCache(ttlCacheDuration)
	return &AvroToJSONTransformer{schemaRegistryEndpoint, client, cache}, nil
}

// getSchemaFromAvroBinary extracts the schema and the message from the Avro binary message and returns them
// It also fetches the schema from the schema registry if the schema is not cached
// It returns the schema string, the schema ID, the message and an error
func (transformer AvroToJSONTransformer) getSchemaFromAvroBinary(msg []byte) (schema string, schemaID int, message []byte, err error) {
	if len(msg) == 0 {
		return "", -1, nil, errors.New("message is empty")
	}

	switch magicByte := msg[0]; magicByte {
	case 0x0: // Standard Magic Avro (Schema ID)

		id := int(binary.BigEndian.Uint32(msg[1:5]))
		messageBinary := msg[5:]

		schema, err = transformer.client.GetSchemaByID(id)
		if err != nil {

			return "", -1, nil, err
		}
		return schema, id, messageBinary, nil

	case 0x1: // Magic Avro (Subject Name)
		var currPos = 1
		subjectSize := int(binary.BigEndian.Uint32(msg[currPos : currPos+4]))
		currPos += 4
		subjectBytes := msg[currPos : currPos+subjectSize]
		currPos += subjectSize
		subjectStr := string(subjectBytes)

		version := int(binary.BigEndian.Uint32(msg[currPos : currPos+4]))
		messageBinary := msg[4+subjectSize+4+1:]

		schema, err := transformer.client.GetSchemaBySubject(subjectStr, version)
		if err != nil {
			return "", -1, nil, err
		}
		return schema.Schema, schema.ID, messageBinary, nil

	default:
		return "", -1, nil, errors.New("magic byte must contains : 0 or 1")
	}
}
