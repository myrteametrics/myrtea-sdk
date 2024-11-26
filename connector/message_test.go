package connector

import (
	"github.com/myrteametrics/myrtea-sdk/v5/expression"
	"testing"
)

func TestKafkaMessageStringReturnsDataAsString(t *testing.T) {
	msg := KafkaMessage{Data: []byte("test data")}
	expression.AssertEqual(t, "test data", msg.String())
}

func TestKafkaMessageGetDataReturnsData(t *testing.T) {
	msg := KafkaMessage{Data: []byte("test data")}
	expression.AssertEqual(t, len([]byte("test data")), len(msg.GetData()))
}

func TestDecodedKafkaMessageStringReturnsEmptyString(t *testing.T) {
	msg := DecodedKafkaMessage{}
	expression.AssertEqual(t, "", msg.String())
}

func TestFilteredJsonMessageStringReturnsEmptyString(t *testing.T) {
	msg := FilteredJsonMessage{}
	expression.AssertEqual(t, "", msg.String())
}

func TestMessageWithOptionsStringReturnsEmptyString(t *testing.T) {
	msg := MessageWithOptions{}
	expression.AssertEqual(t, "", msg.String())
}

func TestTypedDataMessageStringReturnsEmptyString(t *testing.T) {
	msg := TypedDataMessage{}
	expression.AssertEqual(t, "", msg.String())
}
