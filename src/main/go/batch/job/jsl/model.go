package jsl

// Job represents the top-level structure of a JSL file.
type Job struct {
	ID          string      `yaml:"id"`
	Name        string      `yaml:"name"`
	Description string      `yaml:"description,omitempty"`
	Flow        Flow        `yaml:"flow"` // A job must have a flow
	// Other job-level properties like listeners, properties, etc. can be added here.
}

// Flow represents a sequence of steps or decisions.
type Flow struct {
	StartElement string                 `yaml:"start-element"` // The ID of the first step/decision to execute
	Elements     map[string]interface{} `yaml:"elements"`      // Map of element ID to its definition (Step, Decision)
}

// Step represents a single processing unit within a job.
type Step struct {
	ID          string       `yaml:"id"`
	Description string       `yaml:"description,omitempty"`
	Reader      ComponentRef `yaml:"reader"`
	Processor   ComponentRef `yaml:"processor,omitempty"`
	Writer      ComponentRef `yaml:"writer"`
	Chunk       *Chunk       `yaml:"chunk,omitempty"` // Optional chunk configuration
	Transitions []Transition `yaml:"transitions,omitempty"`
	// Other step-level properties like listeners, properties, etc. can be added here.
}

// ComponentRef refers to a registered component (reader, processor, writer).
type ComponentRef struct {
	Ref string `yaml:"ref"` // The name/ID of the component (e.g., "weatherReader", "weatherProcessor")
}

// Chunk defines chunk-oriented processing properties for a step.
type Chunk struct {
	ItemCount int `yaml:"item-count"`
	// Other chunk properties like commit-interval, skip-limit, etc. can be added.
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
