package jsl

// Job represents the top-level structure of a JSL file.
type Job struct {
	ID          string      `yaml:"id"`
	Name        string      `yaml:"name"`
	Description string      `yaml:"description,omitempty"`
	Flow        Flow        `yaml:"flow"` // A job must have a flow
	Listeners   []ComponentRef `yaml:"listeners,omitempty"` // Job-level listeners
	// Other job-level properties like listeners, properties, etc. can be added here.
}

// Flow represents a sequence of steps or decisions.
type Flow struct {
	StartElement string                 `yaml:"start-element"` // The ID of the first step/decision to execute
	Elements     map[string]interface{} `yaml:"elements"`      // Map of element ID to its definition (Step, Decision)
}

// Step represents a single processing unit within a job.
// JSR352では、ステップはチャンク指向またはTasklet指向のいずれかです。
// 両方を同時に持つことはできません。
type Step struct {
	ID          string       `yaml:"id"`
	Description string       `yaml:"description,omitempty"`
	Reader      ComponentRef `yaml:"reader,omitempty"`    // チャンク指向の場合
	Processor   ComponentRef `yaml:"processor,omitempty"` // チャンク指向の場合
	Writer      ComponentRef `yaml:"writer,omitempty"`    // チャンク指向の場合
	Chunk       *Chunk       `yaml:"chunk,omitempty"`     // チャンク指向の場合のチャンク設定
	Tasklet     ComponentRef `yaml:"tasklet,omitempty"`   // Tasklet指向の場合
	Transitions []Transition `yaml:"transitions,omitempty"`
	// Step-level listeners
	Listeners          []ComponentRef `yaml:"listeners,omitempty"`
	ItemReadListeners  []ComponentRef `yaml:"item-read-listeners,omitempty"`
	ItemProcessListeners []ComponentRef `yaml:"item-process-listeners,omitempty"`
	ItemWriteListeners []ComponentRef `yaml:"item-write-listeners,omitempty"`
	SkipListeners      []ComponentRef `yaml:"skip-listeners,omitempty"`
	RetryItemListeners []ComponentRef `yaml:"retry-item-listeners,omitempty"`
	// Other step-level properties like listeners, properties, etc. can be added here.
}

// ComponentRef refers to a registered component (reader, processor, writer, tasklet).
type ComponentRef struct {
	Ref        string            `yaml:"ref"`                 // The name/ID of the component (e.g., "weatherReader", "myTasklet")
	Properties map[string]string `yaml:"properties,omitempty"` // JSLから注入されるプロパティ
}

// Chunk defines chunk-oriented processing properties for a step.
type Chunk struct {
	ItemCount     int `yaml:"item-count"`
	CommitInterval int `yaml:"commit-interval"` // JSR352のcommit-intervalに相当
	// Other chunk properties like skip-limit, etc. can be added.
}

// Decision represents a conditional branching point in the flow.
type Decision struct {
	ID          string       `yaml:"id"`
	Description string       `yaml:"description,omitempty"`
	Transitions []Transition `yaml:"transitions"` // Transitions based on decision outcome
}

// Transition defines the next element to execute based on an exit status.
type Transition struct {
	On   string `yaml:"on"`             // The exit status (e.g., "COMPLETED", "FAILED", "*")
	To   string `yaml:"to,omitempty"`   // The ID of the next element (step or decision)
	End  bool   `yaml:"end,omitempty"`  // If true, ends the job execution
	Fail bool   `yaml:"fail,omitempty"` // If true, fails the job execution
	Stop bool   `yaml:"stop,omitempty"` // If true, stops the job execution
}
