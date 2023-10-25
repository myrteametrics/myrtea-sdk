package variablesconfig

import (
	"strings"

	"go.uber.org/zap"
)

func replaceKeysWithValues(m map[string]string, variables map[string]string) map[string]string {
	result := make(map[string]string)

	for key, value := range m {

		updatedValue := strings.ReplaceAll(value, " ", "")
		for varKey, varValue := range variables {
			pattern := "+{{" +varKey+ "}}+"
			if strings.Contains(updatedValue, pattern) {
				updatedValue = strings.Replace(updatedValue, pattern, varValue, -1)
			}
		}
	
		if strings.Contains(updatedValue, "+{{") && strings.Contains(updatedValue, "}}+") {
			zap.L().Error("Error: Unrecognized variableConfi in", zap.Any("key", key), zap.Any("with value", updatedValue))
		}
	
		result[key] = updatedValue
	}
	

	return result
}
