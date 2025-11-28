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
