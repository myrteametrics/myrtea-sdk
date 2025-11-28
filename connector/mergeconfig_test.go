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

// testMergeWithArrayCheck is a helper that compares arrays as sets (ignoring order)
func testMergeWithArrayCheck(t *testing.T, fieldName string, config Config, new *models.Document, existing *models.Document, expectedArray []interface{}) {
	out := config.Apply(new, existing)

	// Get the field value from output
	actualValue, ok := out.Source[fieldName]
	if !ok {
		t.Errorf("Field %s not found in output", fieldName)
		t.FailNow()
	}

	actualArray, ok := actualValue.([]interface{})
	if !ok {
		t.Errorf("Field %s is not an array", fieldName)
		t.FailNow()
	}

	// Check if arrays have same elements (ignoring order)
	if !arraysHaveSameElements(actualArray, expectedArray) {
		t.Errorf("Arrays don't match for field %s\nActual: %v\nExpected: %v", fieldName, actualArray, expectedArray)
		t.Fail()
	}
}

// arraysHaveSameElements checks if two arrays contain the same elements (ignoring order)
func arraysHaveSameElements(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}

	// Create a map to count occurrences
	counts := make(map[interface{}]int)
	for _, v := range a {
		counts[v]++
	}
	for _, v := range b {
		counts[v]--
		if counts[v] < 0 {
			return false
		}
	}

	// Check all counts are zero
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}

	return true
}

func toMillis(d time.Duration) int64 {
	return d.Nanoseconds() / 1e6
}

func TestNestedGroups(t *testing.T) {
	// Test 1: Basic nested group - parent condition controls nested group execution
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Condition:    "New.status == \"active\"",
					FieldReplace: []string{"status"},
					Groups: []Group{
						{
							FieldReplace: []string{"nestedField"},
						},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"status":      "active",
			"nestedField": "new_nested_value",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"status":      "inactive",
			"nestedField": "existing_nested_value",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"status":      "active",
			"nestedField": "new_nested_value",
		}},
	)

	// Test 2: Nested group not executed when parent condition is false
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Condition:    "New.status == \"inactive\"",
					FieldReplace: []string{"status"},
					Groups: []Group{
						{
							FieldReplace: []string{"nestedField"},
						},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"status":      "active",
			"nestedField": "new_nested_value",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"status":      "inactive",
			"nestedField": "existing_nested_value",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"status":      "inactive",
			"nestedField": "existing_nested_value",
		}},
	)

	// Test 3: Nested group with its own condition
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Condition:    "New.level1 == true",
					FieldReplace: []string{"level1"},
					Groups: []Group{
						{
							Condition:    "New.level2 == true",
							FieldReplace: []string{"level2"},
						},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"level1": true,
			"level2": true,
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"level1": false,
			"level2": false,
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"level1": true,
			"level2": true,
		}},
	)

	// Test 4: Nested group condition is false, parent executes but nested doesn't
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Condition:    "New.level1 == true",
					FieldReplace: []string{"level1"},
					Groups: []Group{
						{
							Condition:    "New.level2 == false",
							FieldReplace: []string{"level2"},
						},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"level1": true,
			"level2": true,
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"level1": false,
			"level2": false,
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"level1": true,
			"level2": false,
		}},
	)

	// Test 5: Multiple nested groups at same level
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Condition:    "New.parent == \"yes\"",
					FieldReplace: []string{"parent"},
					Groups: []Group{
						{
							Condition:    "New.child1 == \"a\"",
							FieldReplace: []string{"child1"},
						},
						{
							Condition:    "New.child2 == \"b\"",
							FieldReplace: []string{"child2"},
						},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"parent": "yes",
			"child1": "a",
			"child2": "b",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"parent": "no",
			"child1": "old_a",
			"child2": "old_b",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"parent": "yes",
			"child1": "a",
			"child2": "b",
		}},
	)

	// Test 6: Deep nesting (3 levels)
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Condition:    "New.level1 == 1",
					FieldReplace: []string{"level1"},
					Groups: []Group{
						{
							Condition:    "New.level2 == 2",
							FieldReplace: []string{"level2"},
							Groups: []Group{
								{
									Condition:    "New.level3 == 3",
									FieldReplace: []string{"level3"},
								},
							},
						},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"level1": 1,
			"level2": 2,
			"level3": 3,
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"level1": 0,
			"level2": 0,
			"level3": 0,
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"level1": 1,
			"level2": 2,
			"level3": 3,
		}},
	)

	// Test 7: Nested groups with FieldMath
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Condition: "New.calculate == true",
					Groups: []Group{
						{
							FieldMath: []FieldMath{
								{Expression: "New.value1 + Existing.value2", OutputField: "result"},
							},
						},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"calculate": true,
			"value1":    10,
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"value2": 20,
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"value2": 20,
			"result": 30,
		}},
	)

	// Test 8: Nested group without condition in parent
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldReplace: []string{"unconditionalField"},
					Groups: []Group{
						{
							Condition:    "New.conditionalField == \"apply\"",
							FieldReplace: []string{"conditionalField"},
						},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"unconditionalField": "new_unconditional",
			"conditionalField":   "apply",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"unconditionalField": "existing_unconditional",
			"conditionalField":   "existing_conditional",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"unconditionalField": "new_unconditional",
			"conditionalField":   "apply",
		}},
	)

	// Test 10: Multiple top-level groups with nested groups
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Condition:    "New.group1Active == true",
					FieldReplace: []string{"group1Field"},
					Groups: []Group{
						{
							FieldReplace: []string{"group1Nested"},
						},
					},
				},
				{
					Condition:    "New.group2Active == true",
					FieldReplace: []string{"group2Field"},
					Groups: []Group{
						{
							FieldReplace: []string{"group2Nested"},
						},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"group1Active": true,
			"group2Active": true,
			"group1Field":  "new1",
			"group1Nested": "newNested1",
			"group2Field":  "new2",
			"group2Nested": "newNested2",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"group1Field":  "existing1",
			"group1Nested": "existingNested1",
			"group2Field":  "existing2",
			"group2Nested": "existingNested2",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"group1Field":  "new1",
			"group1Nested": "newNested1",
			"group2Field":  "new2",
			"group2Nested": "newNested2",
		}},
	)
}

func TestNestedGroupsWithReplaceIfMissing(t *testing.T) {
	// Test nested groups with FieldReplaceIfMissing
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Condition: "New.checkMissing == true",
					Groups: []Group{
						{
							FieldReplaceIfMissing: []string{"optionalField"},
						},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"checkMissing":  true,
			"optionalField": "new_optional",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "existing_value",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "existing_value",
			"optionalField": "new_optional",
		}},
	)
}

func TestNestedGroupsWithForceUpdate(t *testing.T) {
	// Test nested groups with FieldForceUpdate
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Condition: "New.forceUpdate == true",
					Groups: []Group{
						{
							FieldForceUpdate: []string{"updateField"},
						},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"forceUpdate": true,
			"updateField": "new_value",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"updateField": "existing_value",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"updateField": "new_value",
		}},
	)
}

func TestReplaceSimpleField(t *testing.T) {
	// Test basic field replacement
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Replace: []FieldMapping{
						{Source: "sourceField", Destination: "targetField"},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"sourceField": "test_value",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "existing_value",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "existing_value",
			"targetField":   "test_value",
		}},
	)
}

func TestReplaceNestedFields(t *testing.T) {
	// Test nested field replacement
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Replace: []FieldMapping{
						{Source: "data.nested.value", Destination: "output.result"},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"data": map[string]interface{}{
				"nested": map[string]interface{}{
					"value": 12345,
				},
			},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingData": "existing",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingData": "existing",
			"output": map[string]interface{}{
				"result": 12345,
			},
		}},
	)
}

func TestReplaceArrayElement(t *testing.T) {
	// Test replacing from array element to another field
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Replace: []FieldMapping{
						{Source: "items[0]", Destination: "selectedItem"},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"items": []interface{}{"first", "second", "third"},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "existing",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "existing",
			"selectedItem":  "first",
		}},
	)
}

func TestReplaceArrayToArray(t *testing.T) {
	// Test replacing from one array element to another array element
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Replace: []FieldMapping{
						{Source: "source[1]", Destination: "target[0]"},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"source": []interface{}{"a", "b", "c"},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"target": []interface{}{"x", "y", "z"},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"target": []interface{}{"b", "y", "z"},
		}},
	)
}

func TestReplaceNestedArrayObject(t *testing.T) {
	// Test replacing nested object within array
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Replace: []FieldMapping{
						{Source: "users[0].name", Destination: "primaryUser"},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"users": []interface{}{
				map[string]interface{}{"name": "Alice", "age": 30},
				map[string]interface{}{"name": "Bob", "age": 25},
			},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "existing",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "existing",
			"primaryUser":   "Alice",
		}},
	)
}

func TestReplaceComplexArrayMapping(t *testing.T) {
	// Test complex array mapping with nested objects
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Replace: []FieldMapping{
						{Source: "records[1].metadata.timestamp", Destination: "lastUpdate"},
						{Source: "records[0].data.value", Destination: "firstValue"},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"records": []interface{}{
				map[string]interface{}{
					"data": map[string]interface{}{"value": 100},
				},
				map[string]interface{}{
					"metadata": map[string]interface{}{"timestamp": "2025-11-28T10:00:00Z"},
				},
			},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "existing",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "existing",
			"lastUpdate":    "2025-11-28T10:00:00Z",
			"firstValue":    100,
		}},
	)
}

func TestReplaceObjectToArrayElement(t *testing.T) {
	// Test replacing entire object to array element
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Replace: []FieldMapping{
						{Source: "config.settings", Destination: "profiles[0]"},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"config": map[string]interface{}{
				"settings": map[string]interface{}{
					"theme": "dark",
					"lang":  "en",
				},
			},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"profiles": []interface{}{
				map[string]interface{}{"theme": "light", "lang": "fr"},
				map[string]interface{}{"theme": "auto", "lang": "de"},
			},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"profiles": []interface{}{
				map[string]interface{}{"theme": "dark", "lang": "en"},
				map[string]interface{}{"theme": "auto", "lang": "de"},
			},
		}},
	)
}

func TestReplaceWithCondition(t *testing.T) {
	// Test replace with condition
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Condition: "New.shouldReplace == true",
					Replace: []FieldMapping{
						{Source: "newData", Destination: "targetData"},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"shouldReplace": true,
			"newData":       "replacement_value",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"targetData": "original_value",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"targetData": "replacement_value",
		}},
	)
}

func TestReplaceConditionFalse(t *testing.T) {
	// Test replace with false condition - should not replace
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Condition: "New.shouldReplace == true",
					Replace: []FieldMapping{
						{Source: "newData", Destination: "targetData"},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"shouldReplace": false,
			"newData":       "replacement_value",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"targetData": "original_value",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"targetData": "original_value",
		}},
	)
}

func TestReplaceMultipleArrayMappings(t *testing.T) {
	// Test multiple array element replacements
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Replace: []FieldMapping{
						{Source: "source[0].id", Destination: "output[0].identifier"},
						{Source: "source[1].id", Destination: "output[1].identifier"},
						{Source: "source[0].value", Destination: "output[0].data"},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"source": []interface{}{
				map[string]interface{}{"id": "A1", "value": 100},
				map[string]interface{}{"id": "B2", "value": 200},
			},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"output": []interface{}{
				map[string]interface{}{"identifier": "", "data": 0},
				map[string]interface{}{"identifier": "", "data": 0},
			},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"output": []interface{}{
				map[string]interface{}{"identifier": "A1", "data": 100},
				map[string]interface{}{"identifier": "B2", "data": 0},
			},
		}},
	)
}

func TestReplaceNestedGroups(t *testing.T) {
	// Test replace within nested groups
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Condition: "New.processData == true",
					Groups: []Group{
						{
							Replace: []FieldMapping{
								{Source: "temp[0]", Destination: "result"},
							},
						},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"processData": true,
			"temp":        []interface{}{"success", "error"},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"result": "pending",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"result": "success",
		}},
	)
}

func TestReplaceDeepNestedArray(t *testing.T) {
	// Test deep nested array access
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Replace: []FieldMapping{
						{Source: "root.level1[0].level2[1].value", Destination: "extracted"},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"root": map[string]interface{}{
				"level1": []interface{}{
					map[string]interface{}{
						"level2": []interface{}{
							map[string]interface{}{"value": "first"},
							map[string]interface{}{"value": "second"},
						},
					},
				},
			},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "existing",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "existing",
			"extracted":     "second",
		}},
	)
}

func TestReplaceMissingSource(t *testing.T) {
	// Test replace when source field doesn't exist - should not create destination
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Replace: []FieldMapping{
						{Source: "nonExistent", Destination: "target"},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"otherField": "value",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "existing",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "existing",
		}},
	)
}

func TestReplaceArrayOutOfBounds(t *testing.T) {
	// Test replace when array index is out of bounds - should not replace
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Replace: []FieldMapping{
						{Source: "items[10]", Destination: "selected"},
					},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"items": []interface{}{"a", "b", "c"},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"selected": "original",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"selected": "original",
		}},
	)
}

func TestReplaceCombinedWithOtherOperations(t *testing.T) {
	// Test replace combined with other merge operations
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Replace: []FieldMapping{
						{Source: "data[0]", Destination: "primary"},
					},
					FieldReplace: []string{"status"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"data":   []interface{}{"value1", "value2"},
			"status": "active",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"primary": "old_value",
			"status":  "inactive",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"primary": "value1",
			"status":  "active",
		}},
	)
}

func TestApplyFieldMerge(t *testing.T) {
	// Test merging two arrays with unique elements
	testMergeWithArrayCheck(t, "tags",
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMerge: []string{"tags"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"tags": []interface{}{"tag2", "tag3"},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"tags": []interface{}{"tag1", "tag2"},
		}},
		[]interface{}{"tag1", "tag2", "tag3"},
	)

	// Test merging single value with array
	testMergeWithArrayCheck(t, "items",
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMerge: []string{"items"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"items": "newItem",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"items": []interface{}{"existingItem1", "existingItem2"},
		}},
		[]interface{}{"existingItem1", "existingItem2", "newItem"},
	)

	// Test merging array with single value
	testMergeWithArrayCheck(t, "items",
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMerge: []string{"items"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"items": []interface{}{"newItem1", "newItem2"},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"items": "existingItem",
		}},
		[]interface{}{"existingItem", "newItem1", "newItem2"},
	)

	// Test merging two single values
	testMergeWithArrayCheck(t, "value",
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMerge: []string{"value"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"value": "newValue",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"value": "existingValue",
		}},
		[]interface{}{"existingValue", "newValue"},
	)

	// Test merging arrays with duplicates - should remove duplicates
	testMergeWithArrayCheck(t, "categories",
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMerge: []string{"categories"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"categories": []interface{}{"cat1", "cat2", "cat3"},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"categories": []interface{}{"cat2", "cat3", "cat4"},
		}},
		[]interface{}{"cat1", "cat2", "cat3", "cat4"},
	)

	// Test merging when enricher field doesn't exist - should not merge
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMerge: []string{"nonExistent"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"otherField": "value",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "existing",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "existing",
		}},
	)

	// Test merging with ExistingAsMaster = false
	testMergeWithArrayCheck(t, "tags",
		Config{Type: "doc", Mode: Self, ExistingAsMaster: false,
			Groups: []Group{
				{
					FieldMerge: []string{"tags"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"tags": []interface{}{"newTag1", "newTag2"},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"tags": []interface{}{"existingTag1", "existingTag2"},
		}},
		[]interface{}{"newTag1", "newTag2", "existingTag1", "existingTag2"},
	)

	// Test merging numeric values
	testMergeWithArrayCheck(t, "numbers",
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldMerge: []string{"numbers"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"numbers": []interface{}{1, 2, 3},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"numbers": []interface{}{2, 3, 4, 5},
		}},
		[]interface{}{1, 2, 3, 4, 5},
	)

	// Test merging with condition
	testMergeWithArrayCheck(t, "items",
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					Condition:  "New.shouldMerge == true",
					FieldMerge: []string{"items"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"shouldMerge": true,
			"items":       []interface{}{"item3", "item4"},
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"items": []interface{}{"item1", "item2"},
		}},
		[]interface{}{"item1", "item2", "item3", "item4"},
	)
}

func TestApplyFieldKeepEarliestEdgeCases(t *testing.T) {
	// Test when enricher source field doesn't exist - should not update
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldKeepEarliest: []string{"timestamp"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"otherField": "value",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"timestamp": "2019-11-20T12:00:25+02:00",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"timestamp": "2019-11-20T12:00:25+02:00",
		}},
	)

	// Test when enricher source has invalid date format - should not update
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldKeepEarliest: []string{"timestamp"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"timestamp": "not-a-date",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"timestamp": "2019-11-20T12:00:25+02:00",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"timestamp": "2019-11-20T12:00:25+02:00",
		}},
	)

	// Test when output has invalid date but enricher has valid date - should update
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldKeepEarliest: []string{"timestamp"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"timestamp": "2019-10-20T12:00:25+02:00",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"timestamp": "invalid-date",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"timestamp": "2019-10-20T12:00:25+02:00",
		}},
	)

	// Test when output doesn't have field and enricher does - should add
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldKeepEarliest: []string{"newTimestamp"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"newTimestamp": "2019-11-20T12:00:25+02:00",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "value",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"existingField": "value",
			"newTimestamp":  "2019-11-20T12:00:25+02:00",
		}},
	)

	// Test when output has earlier date (should NOT update - keep existing earlier)
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldKeepEarliest: []string{"timestamp"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"timestamp": "2019-12-20T12:00:25+02:00",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"timestamp": "2019-10-20T12:00:25+02:00",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"timestamp": "2019-10-20T12:00:25+02:00",
		}},
	)

	// Test with various date formats
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldKeepEarliest: []string{"date"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"date": "2019-10-20",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"date": "2019-11-20",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"date": "2019-10-20",
		}},
	)

	// Test with RFC3339 format
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldKeepEarliest: []string{"created"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"created": "2019-01-15T10:00:00Z",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"created": "2019-12-15T10:00:00Z",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"created": "2019-01-15T10:00:00Z",
		}},
	)

	// Test with empty string in enricher - should not update
	testMerge(t,
		Config{Type: "doc", Mode: Self, ExistingAsMaster: true,
			Groups: []Group{
				{
					FieldKeepEarliest: []string{"timestamp"},
				},
			},
		},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"timestamp": "",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"timestamp": "2019-11-20T12:00:25+02:00",
		}},
		&models.Document{ID: "1", IndexType: "doc", Source: map[string]interface{}{
			"timestamp": "2019-11-20T12:00:25+02:00",
		}},
	)
}
