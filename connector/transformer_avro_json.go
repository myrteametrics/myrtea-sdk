package connector

import (
	"encoding/binary"
	"errors"

	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/linkedin/goavro"
	ttlcache "github.com/myrteametrics/myrtea-sdk/v5/cache"
	"github.com/myrteametrics/myrtea-sdk/v5/utils"
)

// AvroToJSONTransformer :
// Deprecated
type AvroToJSONTransformer struct {
	schemaRegistryEndpoint string
	client                 *utils.CachedSchemaRegistry
	cache                  *ttlcache.Cache
}

// Transform is the convertor transformer, it has to decode the AVRO message into a byte message (JSONMessage)
// Deprecated
func (transformer AvroToJSONTransformer) Transform(msg Message) (Message, error) {
	switch kafkaMsg := msg.(type) {
	case KafkaMessage:
		textual, err := transformer.AvroBinaryToTextual(kafkaMsg.Data)
		if err != nil {
			zap.L().Info("transformer.AvroBinaryToTextual() : ", zap.Error(err))
			return nil, errors.New("couldn't convert the AVRO binary to a TextualBinary (JSONMessage)")
		}
		return KafkaMessage{Data: textual}, nil

	default:
		return nil, errors.New("couldn't transform the Message, the convertor transformer couldn't get the Type of the incoming message")
	}
}

// NewAvroToJSONTransformer New transformer constructor TODO : Manage multiple schemaRegistryEndpoint ? In case of server failure ?
// Deprecated
func NewAvroToJSONTransformer(schemaRegistryEndpoint string, ttlCacheDuration time.Duration) (*AvroToJSONTransformer, error) {
	client, err := utils.NewCachedSchemaRegistry(schemaRegistryEndpoint, ttlCacheDuration)
	if err != nil {
		return nil, err
	}
	cache := ttlcache.NewCache(ttlCacheDuration)
	return &AvroToJSONTransformer{schemaRegistryEndpoint, client, cache}, nil
}

// AvroBinaryToNative :
// Deprecated
func (transformer AvroToJSONTransformer) AvroBinaryToNative(avroBinary []byte) (interface{}, error) {
	codec, msg, err := transformer.exposeAvroBinary(avroBinary)
	if err != nil {
		zap.L().Error("transformer.exposeAvroBinary() :", zap.Error(err))
		return nil, err
	}

	native, _, err := codec.NativeFromBinary(msg)
	if err != nil {
		zap.L().Info("transformer.AvroBinaryToNative()", zap.Error(err))
		return nil, err
	}

	return native, nil
}

// AvroBinaryToTextual :
// Deprecated
func (transformer AvroToJSONTransformer) AvroBinaryToTextual(avroBinary []byte) ([]byte, error) {

	codec, msg, err := transformer.exposeAvroBinary(avroBinary)
	if err != nil {
		zap.L().Info("transformer.exposeAvroBinary()", zap.Error(err))
		return nil, err
	}

	native, _, err := codec.NativeFromBinary(msg)
	if err != nil {
		zap.L().Info("codec.NativeFromBinary()", zap.Error(err))
		return nil, err
	}

	textual, err := codec.TextualFromNative(nil, native)
	if err != nil {
		zap.L().Info("codec.TextualFromNative()", zap.Error(err))
		return nil, err
	}

	return textual, nil
}

// getCodec
// Deprecated
func (transformer AvroToJSONTransformer) getCodec(id int, schema string) (*goavro.Codec, error) {
	idStr := strconv.Itoa(id)
	value, exists := transformer.cache.Get(idStr)
	if exists {
		//zap.L().Debug("codec from cache")
		return value.(*goavro.Codec), nil
	}
	//zap.L().Debug("codec from server build")

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}

	transformer.cache.Set(idStr, codec)
	return codec, nil
}

// exposeAvroBinary :
// Deprecated
func (transformer AvroToJSONTransformer) exposeAvroBinary(avroBinary []byte) (*goavro.Codec, []byte, error) {
	schema, schemaID, msg, err := transformer.getSchemaFromAvroBinary(avroBinary)
	if err != nil {
		return nil, nil, err
	}
	codec, err := transformer.getCodec(schemaID, schema)
	if err != nil {
		return nil, nil, err
	}

	return codec, msg, nil
}

// getSchemaFromAvroBinary :
// Deprecated
func (transformer AvroToJSONTransformer) getSchemaFromAvroBinary(msg []byte) (string, int, []byte, error) {
	if len(msg) == 0 {
		return "", -1, nil, errors.New("message is empty")
	}

	switch magicByte := msg[0]; magicByte {
	case 0x0: // Standard Magic Avro (Schema ID)

		id := int(binary.BigEndian.Uint32(msg[1:5]))
		messageBinary := msg[5:]

		schema, err := transformer.client.GetSchemaByID(id)
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
