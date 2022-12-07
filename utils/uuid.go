package utils

import (
	"fmt"
)

// New return a formatted UUID based on two int64
func NewUUIDFromBits(uuidMostSig int64, uuidLeastSig int64) string {
	return digits(uuidMostSig>>32, 8) + "-" +
		digits(uuidMostSig>>16, 4) + "-" +
		digits(uuidMostSig, 4) + "-" +
		digits(uuidLeastSig>>48, 4) + "-" +
		digits(uuidLeastSig, 12)
}

func digits(val int64, digits uint) string {
	var hi int64 = 1 << (digits * 4)
	out := hi | (val & (hi - 1))
	outStr := fmt.Sprintf("%x", out)
	return outStr[1:]
}
