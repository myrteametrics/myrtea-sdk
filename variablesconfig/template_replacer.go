package variablesconfig

import (
	"strings"

	"go.uber.org/zap"
)

func ReplaceKeysWithValues(m *map[string]string, variables map[string]string) {
    for key, value := range *m {
        updatedValue := strings.ReplaceAll(value, " ", "")
        for varKey, varValue := range variables {
            pattern1 := "+{{" + varKey + "}}+"
            pattern2 := "{{" + varKey + "}}+"
            pattern3 := "+{{" + varKey + "}}"
            pattern4 := "{{" + varKey + "}}"
			
            updatedValue = strings.ReplaceAll(updatedValue, pattern1, varValue)
            updatedValue = strings.ReplaceAll(updatedValue, pattern2, varValue)
            updatedValue = strings.ReplaceAll(updatedValue, pattern3, varValue)
            updatedValue = strings.ReplaceAll(updatedValue, pattern4, varValue)
        }

        if strings.Contains(updatedValue, "+{{") || strings.Contains(updatedValue, "}}+") || strings.Contains(updatedValue, "{{") || strings.Contains(updatedValue, "}}") {
            zap.L().Error("Error: Unrecognized variableConfi in", zap.Any("key", key), zap.Any("with value", updatedValue))
        }

        (*m)[key] = updatedValue
    }
}
