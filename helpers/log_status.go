package helpers


type Status int 

const (
	// Initialized is used for initialization
	Initialized Status = iota + 900
	// Started is used for begin a task 
	Started
	// InProgress is used to mention that the exexution is in progress
	InProgress
	// Finished is used to mention that the exexution is done
	Finished
)

var toString = map[Status]string{
	Initialized:  "initialized",
	Started:   	  "started",
	InProgress:   "inProgress",
	Finished: 	  "finished",
}

var toID = map[string]Status{
	"initialized": Initialized,
	"started":     Started,
	"inProgress":  InProgress,
	"finished":    Finished,
}