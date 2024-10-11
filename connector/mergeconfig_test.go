package connector

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/myrteametrics/myrtea-sdk/v5/models"
)

func TestMergeMath(t *testing.T) {
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Condition: "datemillis(New.DT_ITMATT_IMP) < datemillis(Existing.DT_ITMATT_IMP)",
					FieldReplace: []string{
						"DT_ITMATT_IMP",
					},
				},
				{
					Condition: "datemillis(New.DT_ITMATT_EXP) < datemillis(Existing.DT_ITMATT_EXP)",
					FieldReplace: []string{
						"DT_ITMATT_EXP",
					},
				},
				{
					Condition: "datemillis(Existing.DT_DEADLINE_ITMATT_EXP) < datemillis(New.DT_ITMATT_EXP)",
					FieldMath: []FieldMath{
						{Expression: "\"LATE\"", OutputField: "ITMATT_EXP_STATUS"},
					},
				},
				{
					Condition: "datemillis(Existing.DT_DEADLINE_ITMATT_EXP) >= datemillis(New.DT_ITMATT_EXP)",
					FieldMath: []FieldMath{
						{Expression: "\"OK\"", OutputField: "ITMATT_EXP_STATUS"},
					},
				},
				{
					FieldReplaceIfMissing: []string{
						"ID_COLIS",
						"DT_ITMATT_IMP",
						"DT_ITMATT_EXP",
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"ID_COLIS":      "1",
			"DT_ITMATT_EXP": "2021-07-19T06:50:00.000",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"ID_COLIS":               "1",
			"DT_FLASH_MLVEXP":        "2021-07-19T06:50:00.000",
			"DT_DEADLINE_ITMATT_EXP": "2021-07-20T14:00:00.000",
			"ITMATT_EXP_STATUS":      "MISSING",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"ID_COLIS":               "1",
			"DT_FLASH_MLVEXP":        "2021-07-19T06:50:00.000",
			"DT_DEADLINE_ITMATT_EXP": "2021-07-20T14:00:00.000",
			"DT_ITMATT_EXP":          "2021-07-19T06:50:00.000",
			"ITMATT_EXP_STATUS":      "OK",
		}},
	)
}

func TestMergeMathReverse(t *testing.T) {
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				// {
				// 	Condition: "datemillis(New.DT_FLASH_MLVEXP) < datemillis(Existing.DT_FLASH_MLVEXP)",
				// 	FieldReplace: []string{
				// 		"DT_FLASH_MLVEXP",
				// 		"DT_DEADLINE_ITMATT_EXP",
				// 	},
				// },
				{
					Condition: "datemillis(New.DT_DEADLINE_ITMATT_EXP) < datemillis(Existing.DT_ITMATT_EXP)",
					FieldMath: []FieldMath{
						{Expression: "\"LATE\"", OutputField: "ITMATT_EXP_STATUS"},
					},
				},
				{
					Condition: "datemillis(New.DT_DEADLINE_ITMATT_EXP) >= datemillis(Existing.DT_ITMATT_EXP)",
					FieldMath: []FieldMath{
						{Expression: "\"OK\"", OutputField: "ITMATT_EXP_STATUS"},
					},
				},
				{
					FieldReplaceIfMissing: []string{
						"ID_COLIS",
						"DT_FLASH_MLVEXP",
						"DT_DEADLINE_ITMATT_EXP",
						"ITMATT_EXP_STATUS",
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"ID_COLIS":               "1",
			"DT_FLASH_MLVEXP":        "2021-07-19T06:50:00.000",
			"DT_DEADLINE_ITMATT_EXP": "2021-07-20T14:00:00.000",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"ID_COLIS":          "1",
			"DT_ITMATT_EXP":     "2021-07-19T06:50:00.000",
			"ITMATT_EXP_STATUS": "MISSING",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"ID_COLIS":               "1",
			"DT_FLASH_MLVEXP":        "2021-07-19T06:50:00.000",
			"DT_DEADLINE_ITMATT_EXP": "2021-07-20T14:00:00.000",
			"DT_ITMATT_EXP":          "2021-07-19T06:50:00.000",
			"ITMATT_EXP_STATUS":      "OK",
		}},
	)

}

func TestMergeConfigReplace(t *testing.T) {
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{FieldReplace: []string{"a"}},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": "new_value"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": "existing_value"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": "new_value"}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{FieldReplace: []string{"a.b"}},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "new_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "new_value"}}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{FieldReplace: []string{"a.b", "c.d"}},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"c": map[string]interface{}{"d": "new_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}, "c": map[string]interface{}{"d": "new_value"}}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{FieldReplace: []string{"a.b", "c.d"}},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"c": map[string]interface{}{"d": "new_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}, "c": map[string]interface{}{"e": "existing_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}, "c": map[string]interface{}{"e": "existing_value", "d": "new_value"}}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{FieldReplace: []string{"a.b", "c.d"}},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": "new_value"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}}},
	)
}

func TestMergeConfigReplaceIfMissing(t *testing.T) {

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{FieldReplaceIfMissing: []string{"b"}},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"b": "new_value"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": "existing_value"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": "existing_value", "b": "new_value"}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{FieldReplaceIfMissing: []string{"a.c"}},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"c": "new_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value", "c": "new_value"}}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{FieldReplaceIfMissing: []string{"a"}},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": "new_value"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": "existing_value"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": "existing_value"}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{FieldReplaceIfMissing: []string{"a.b"}},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "new_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{FieldReplaceIfMissing: []string{"a"}},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": "new_value"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}}},
	)
}

func TestMergeConfigDateArithmeticWorkingDays(t *testing.T) {

	// zapConfig := zap.NewDevelopmentConfig()
	// logger, err := zapConfig.Build(zap.AddStacktrace(zap.ErrorLevel))
	// if err != nil {
	// 	log.Fatalf("can't initialize zap logger: %v", err)
	// }
	// defer logger.Sync()
	// zap.ReplaceGlobals(logger)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMath: []FieldMath{
						{Expression: `calendar_delay_od(Existing.date1, New.date2)`, OutputField: "delayWorkingDays"},
					},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"date2": "2019-12-17T10:55:07.000"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-12-17T10:11:49.000"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-12-17T10:11:49.000",
			"delayWorkingDays": toMillis(43*time.Minute + 18*time.Second), // Remove May 8th
		}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMath: []FieldMath{
						{Expression: `datemillis(New.date1) - datemillis(Existing.date1)`, OutputField: "delay"},
						{Expression: `calendar_delay_od(Existing.date1, New.date1)`, OutputField: "delayWorkingDays"},
					},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-05-10T12:10:25.000+02:00"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-05-06T12:00:25.000+02:00"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-05-06T12:00:25.000+02:00",
			"delay":            toMillis(4*24*time.Hour + 10*time.Minute),
			"delayWorkingDays": toMillis(4*24*time.Hour + 10*time.Minute - 1*24*time.Hour), // Remove May 8th
		}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMath: []FieldMath{
						{Expression: `calendar_add_od(New.date1, "-24h")`, OutputField: "dateWorkingDays"},
					},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-05-10T12:10:25.000+02:00"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-05-06T12:00:25.000+02:00"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-05-06T12:00:25.000+02:00",
			"dateWorkingDays": "2019-05-09T12:10:25.000",
		}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMath: []FieldMath{
						{Expression: `calendar_add_od(Existing.date1, New.duration)`, OutputField: "dateWorkingDays"},
					},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"duration": "-72h"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-05-10T12:10:25.000+02:00"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-05-10T12:10:25.000+02:00",
			"dateWorkingDays": "2019-05-06T12:10:25.000",
		}},
	)

	// testMerge(t,
	// 	Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
	// 		Groups: []Group{
	// 			Group{
	// 				FieldMath: []FieldMath{
	// 					FieldMath{Expression: `calendar_delay_od(Existing.date1, calendar_add_od(New.date1, "-24h"))`, OutputField: "delayWorkingDays"},
	// 				},
	// 			},
	// 		},
	// 	},
	// 	&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-05-10T12:10:25.000+02:00"}},
	// 	&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-05-06T12:00:25.000+02:00"}},
	// 	&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-05-06T12:00:25.000+02:00",
	// 		"delayWorkingDays": toMillis(4*24*time.Hour + 10*time.Minute - 1*24*time.Hour - 1*24*time.Hour),
	// 	}},
	// )
}

func TestMergeConfigArithmetic(t *testing.T) {

	// zapConfig := zap.NewDevelopmentConfig()
	// logger, err := zapConfig.Build(zap.AddStacktrace(zap.ErrorLevel))
	// if err != nil {
	// 	log.Fatalf("can't initialize zap logger: %v", err)
	// }
	// defer logger.Sync()
	// zap.ReplaceGlobals(logger)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: false,
			Groups: []Group{
				{
					FieldMath: []FieldMath{
						{Expression: `New.a - Existing.a`, OutputField: "a"},
					},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": 10, "b": map[string]interface{}{"c": 100}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": 4, "b": map[string]interface{}{"c": 30}}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": 6, "b": map[string]interface{}{"c": 100}}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMath: []FieldMath{
						{Expression: `New.b.c - Existing.b.c`, OutputField: "math"},
					},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": 10, "b": map[string]interface{}{"c": 100}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": 4, "b": map[string]interface{}{"c": 30}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": 4, "b": map[string]interface{}{"c": 30}, "math": 70}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMath: []FieldMath{
						{Expression: `Existing.b.c - New.a`, OutputField: "math"}},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": 10, "b": map[string]interface{}{"c": 100}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": 4, "b": map[string]interface{}{"c": 30}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": 4, "b": map[string]interface{}{"c": 30}, "math": 20}},
	)
}

func TestMergeConfigDateArithmetic(t *testing.T) {

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMath: []FieldMath{
						{Expression: `datemillis(New.date1) - datemillis(Existing.date1)`, OutputField: "delay"}},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-11-20T12:10:25.000+02:00"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-11-20T12:00:25.000+02:00"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-11-20T12:00:25.000+02:00", "delay": toMillis(time.Duration(10) * time.Minute)}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMath: []FieldMath{{Expression: `(datemillis(New.date1) - datemillis(Existing.date1)) / 1000`, OutputField: "delay"}},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-11-20T12:10:25.000+02:00"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-11-20T12:00:25.000+02:00"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-11-20T12:00:25.000+02:00", "delay": (time.Duration(10) * time.Minute).Seconds()}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMath:    []FieldMath{{Expression: `datemillis(New.date1) - datemillis(Existing.date1)`, OutputField: "delay"}},
					FieldReplace: []string{"delay"},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-11-20T12:10:25.000+02:00", "delay": toMillis(time.Duration(20) * time.Second)}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-11-20T12:00:25.000+02:00", "delay": 0}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date1": "2019-11-20T12:00:25.000+02:00", "delay": toMillis(time.Duration(20) * time.Second)}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMath:    []FieldMath{{Expression: `datemillis(New.date_output) - datemillis(Existing.date_input)`, OutputField: "delay"}},
					FieldReplace: []string{"date_output"},
				}},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"date_output": "2019-11-20T12:10:25.000+02:00", "c": "dont_add_me"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date_input": "2019-11-20T12:00:25.000+02:00", "d": "add_me"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date_input": "2019-11-20T12:00:25.000+02:00", "date_output": "2019-11-20T12:10:25.000+02:00", "d": "add_me", "delay": toMillis(time.Duration(10) * time.Minute)}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: false,
			Groups: []Group{
				{
					FieldMath:    []FieldMath{{Expression: `datemillis(Existing.date_output) - datemillis(New.date_input)`, OutputField: "delay"}},
					FieldReplace: []string{"date_output"},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"date_input": "2019-11-20T12:00:25.000+02:00", "c": "add_me"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"date_output": "2019-11-20T12:10:25.000+02:00", "c": "replace_me", "d": "dont_add_me"}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"date_input": "2019-11-20T12:00:25.000+02:00", "date_output": "2019-11-20T12:10:25.000+02:00", "c": "add_me", "delay": toMillis(time.Duration(10) * time.Minute)}},
	)
}

//func TestMergeConfigFieldMerge(t *testing.T) {
//	testMerge(t,
//		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
//			Groups: []Group{
//				{
//					FieldMerge: []string{"a"},
//				}},
//		},
//		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": []interface{}{"value2"}}},
//		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": []interface{}{"value1"}}},
//		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": []interface{}{"value1", "value2"}}},
//	)
//
//	testMerge(t,
//		Config{Type: "doc", Mode: Self, ExistingAsMaster: false,
//			Groups: []Group{
//				{
//					FieldMerge: []string{"a"},
//				},
//			},
//		},
//		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": []interface{}{"value1"}}},
//		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": []interface{}{"value2"}}},
//		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": []interface{}{"value1", "value2"}}},
//	)
//}

func TestMergeConfigPartial(t *testing.T) {
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: false,
			Groups: []Group{
				{
					FieldMath: []FieldMath{{Expression: `New.a - Existing.a`, OutputField: "a"}},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": 10, "b": map[string]interface{}{"c": 100}}},
		nil,
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": 10, "b": map[string]interface{}{"c": 100}}},
	)
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: false,
			Groups: []Group{
				{
					FieldMath: []FieldMath{{Expression: `New.a - Existing.a`, OutputField: "a"}},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": 10, "b": map[string]interface{}{"c": 100}}},
		&models.Document{ID: "1", IndexType: "doc", Source: nil},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": 10, "b": map[string]interface{}{"c": 100}}},
	)
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: false,
			Groups: []Group{
				{
					FieldMath: []FieldMath{{Expression: `New.a - Existing.a`, OutputField: "a"}},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": 10, "b": map[string]interface{}{"c": 100}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": 10, "b": map[string]interface{}{"c": 100}}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMath: []FieldMath{{Expression: `New.a - Existing.a`, OutputField: "a"}},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": 10, "b": map[string]interface{}{"c": 100}}},
		nil,
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": 10, "b": map[string]interface{}{"c": 100}}},
	)
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMath: []FieldMath{{Expression: `New.a - Existing.a`, OutputField: "a"}},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": 10, "b": map[string]interface{}{"c": 100}}},
		&models.Document{ID: "1", IndexType: "doc", Source: nil},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{}},
	)
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMath: []FieldMath{{Expression: `New.a - Existing.a`, OutputField: "a"}},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": 10, "b": map[string]interface{}{"c": 100}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: false,
			Groups: []Group{
				{
					FieldMath: []FieldMath{{Expression: `New.a - Existing.a`, OutputField: "a"}},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: nil},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": 4, "b": map[string]interface{}{"c": 30}}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: false,
			Groups: []Group{
				{
					FieldMath: []FieldMath{{Expression: `New.a - Existing.a`, OutputField: "a"}},
				},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": 4, "b": map[string]interface{}{"c": 30}}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{}},
	)
}

func TestApplyFieldKeepEarliest(t *testing.T) {

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldKeepEarliest: []string{"a"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": "2019-11-20T12:00:25+02:00"}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": "2019-10-20T12:00:25+02:00"}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": "2019-10-20T12:00:25+02:00"}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: false,
			Groups: []Group{
				{
					FieldKeepEarliest: []string{"a"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": "2019-10-20T12:00:25+02:00"}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": "2019-11-20T12:00:25+02:00"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": "2019-10-20T12:00:25+02:00"}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldKeepEarliest: []string{"a.b"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "2019-11-20T12:00:25+02:00"}}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "2019-10-20T12:00:25+02:00"}}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "2019-10-20T12:00:25+02:00"}}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: false,
			Groups: []Group{
				{
					FieldKeepEarliest: []string{"a.b.c"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": "2019-10-20T12:00:25+02:00"}}}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": "2019-11-20T12:00:25+02:00"}}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": "2019-10-20T12:00:25+02:00"}}}},
	)
}

func TestFieldKeepLatest(t *testing.T) {

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldKeepLatest: []string{"a"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": "2019-11-20T12:00:25+02:00"}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": "2019-10-20T12:00:25+02:00"}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": "2019-11-20T12:00:25+02:00"}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: false,
			Groups: []Group{
				{
					FieldKeepLatest: []string{"a"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": "2019-10-20T12:00:25+02:00"}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": "2019-11-20T12:00:25+02:00"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": "2019-11-20T12:00:25+02:00"}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldKeepLatest: []string{"a.b"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "2019-11-20T12:00:25+02:00"}}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "2019-10-20T12:00:25+02:00"}}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "2019-11-20T12:00:25+02:00"}}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: false,
			Groups: []Group{
				{
					FieldKeepLatest: []string{"a.b.c"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": "2019-10-20T12:00:25+02:00"}}}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": "2019-11-20T12:00:25+02:00"}}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": "2019-11-20T12:00:25+02:00"}}}},
	)
}

func TestFieldMergeArray(t *testing.T) {
	t.SkipNow() // issue with array order
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMerge: []string{"a"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": []interface{}{"test1"}}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": []interface{}{"test2", "test1"}}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": []interface{}{"test2", "test1"}}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMerge: []string{"a"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": "test1"}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": "test2"}},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": []interface{}{"test2", "test1"}}},
	)
}

func TestMergeForceUpdate(t *testing.T) {
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{FieldForceUpdate: []string{"a"}},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": ""}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": "existing_value"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{FieldForceUpdate: []string{"a.b"}},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": ""}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{}}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{FieldForceUpdate: []string{"a.b", "c.d"}},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"c": map[string]interface{}{"d": "new_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}, "c": map[string]interface{}{"d": "new_value"}}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{FieldForceUpdate: []string{"a.b", "c.d"}},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"c": map[string]interface{}{"d": "new_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}, "c": map[string]interface{}{"e": "existing_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}, "c": map[string]interface{}{"e": "existing_value", "d": "new_value"}}},
	)

	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{FieldForceUpdate: []string{"a.b", "c.d"}},
			},
		},
		&models.Document{ID: "2", IndexType: "doc", Source: map[string]interface{}{"a": "new_value"}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{"a": map[string]interface{}{"b": "existing_value"}}},
	)
}

func testMerge(t *testing.T, config Config, new *models.Document, existing *models.Document, expected *models.Document) *models.Document {
	out := config.Apply(new, existing)
	outJSON, _ := json.Marshal(*out)
	expectedJSON, _ := json.Marshal(expected)

	if string(outJSON) != string(expectedJSON) {
		t.Error("invalid merge")
		t.Fail()
		t.Log(out)
		t.Log(expected)
		return nil
	}
	return out
}

func toMillis(d time.Duration) int64 {
	return d.Nanoseconds() / 1e6
}
