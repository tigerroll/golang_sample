package jsl

import (
	"embed"
	"fmt"
	"path/filepath"
	"reflect"

	"gopkg.in/yaml.v3"

	"sample/src/main/go/batch/job/core"
	"sample/src/main/go/batch/step/processor"
	"sample/src/main/go/batch/step/reader"
	"sample/src/main/go/batch/step/writer"
	exception "sample/src/main/go/batch/util/exception" // Alias for clarity
	logger "sample/src/main/go/batch/util/logger"       // Alias for clarity
)

//go:embed *.yaml
var jslFS embed.FS

// LoadedJobDefinitions holds all loaded JSL job definitions.
var LoadedJobDefinitions = make(map[string]Job)

// LoadJSLDefinitions loads all JSL YAML files embedded in the binary.
func LoadJSLDefinitions() error {
	logger.Infof("JSL 定義のロードを開始します。")
	files, err := jslFS.ReadDir(".")
	if err != nil {
		return exception.NewBatchError("jsl_loader", "JSL ディレクトリの読み込みに失敗しました", err)
	}

	for _, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".yaml" {
			continue
		}

		filePath := file.Name()
		logger.Debugf("JSL ファイルを読み込み中: %s", filePath)
		data, err := jslFS.ReadFile(filePath)
		if err != nil {
			return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ファイル '%s' の読み込みに失敗しました", filePath), err)
		}

		var jobDef Job
		if err := yaml.Unmarshal(data, &jobDef); err != nil {
			return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ファイル '%s' のパースに失敗しました", filePath), err)
		}

		if jobDef.ID == "" {
			return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ファイル '%s' に 'id' が定義されていません", filePath), nil)
		}

		if _, exists := LoadedJobDefinitions[jobDef.ID]; exists {
			return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ジョブID '%s' が重複しています", jobDef.ID), nil)
		}

		LoadedJobDefinitions[jobDef.ID] = jobDef
		logger.Infof("JSL ジョブ '%s' をロードしました。", jobDef.ID)
	}
	logger.Infof("JSL 定義のロードが完了しました。ロードされたジョブ数: %d", len(LoadedJobDefinitions))
	return nil
}

// GetJobDefinition retrieves a JSL Job definition by its ID.
func GetJobDefinition(jobID string) (Job, bool) {
	job, ok := LoadedJobDefinitions[jobID]
	return job, ok
}

// ConvertJSLToCoreFlow converts a JSL Flow definition into a core.FlowDefinition.
// componentRegistry maps string names (from JSL) to actual Go component instances (Reader, Processor, Writer).
func ConvertJSLToCoreFlow(jslFlow Flow, componentRegistry map[string]interface{}) (*core.FlowDefinition, error) {
	coreFlow := &core.FlowDefinition{
		StartElement:    jslFlow.StartElement,
		Elements:        make(map[string]interface{}),
		TransitionRules: make([]core.TransitionRule, 0), // Initialize TransitionRules
	}

	for id, elem := range jslFlow.Elements {
		// YAML unmarshaling of interface{} often results in map[interface{}]interface{} or map[string]interface{}.
		// We need to re-marshal and unmarshal to the specific JSL type (Step or Decision).
		elemBytes, err := yaml.Marshal(elem)
		if err != nil {
			return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("フロー要素 '%s' の再マーシャルに失敗しました", id), err)
		}

		// Try to unmarshal as Step
		var jslStep Step
		if err := yaml.Unmarshal(elemBytes, &jslStep); err == nil && jslStep.ID != "" && jslStep.Reader.Ref != "" && jslStep.Writer.Ref != "" {
			// Successfully unmarshaled as Step
			coreStep, err := convertJSLStepToCoreStep(jslStep, componentRegistry)
			if err != nil {
				return nil, err
			}
			coreFlow.Elements[id] = coreStep
			// Add transitions for this step
			for _, t := range jslStep.Transitions {
				coreFlow.AddTransitionRule(id, t.On, t.To, t.End, t.Fail, t.Stop) // Use core.FlowDefinition.AddTransitionRule
			}
			continue
		}

		// Try to unmarshal as Decision
		var jslDecision Decision
		if err := yaml.Unmarshal(elemBytes, &jslDecision); err == nil && jslDecision.ID != "" && len(jslDecision.Transitions) > 0 {
			// Successfully unmarshaled as Decision
			// For now, we'll store the JSL Decision directly.
			// A proper core.Decision implementation would be needed for execution.
			// TODO: ここで jsl.Decision を core.Decision の具体的な実装に変換する
			coreFlow.Elements[id] = jslDecision // Placeholder for core.Decision
			// Add transitions for this decision
			for _, t := range jslDecision.Transitions {
				coreFlow.AddTransitionRule(id, t.On, t.To, t.End, t.Fail, t.Stop) // Use core.FlowDefinition.AddTransitionRule
			}
			continue
		}

		return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("不明なフロー要素の型または必須フィールドが不足しています (ID: %s, Type: %s)", id, reflect.TypeOf(elem)), nil)
	}
	return coreFlow, nil
}

// convertJSLStepToCoreStep converts a JSL Step definition to a concrete core.Step implementation.
func convertJSLStepToCoreStep(jslStep Step, componentRegistry map[string]interface{}) (core.Step, error) {
	r, ok := componentRegistry[jslStep.Reader.Ref].(reader.Reader)
	if !ok {
		return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("リーダー '%s' が見つからないか、不正な型です", jslStep.Reader.Ref), nil)
	}

	var p processor.Processor
	if jslStep.Processor.Ref != "" {
		p, ok = componentRegistry[jslStep.Processor.Ref].(processor.Processor)
		if !ok {
			return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("プロセッサー '%s' が見つからないか、不正な型です", jslStep.Processor.Ref), nil)
		}
	}

	w, ok := componentRegistry[jslStep.Writer.Ref].(writer.Writer)
	if !ok {
		return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("ライター '%s' が見つからないか、不正な型です", jslStep.Writer.Ref), nil)
	}

	chunkSize := 1 // Default chunk size
	if jslStep.Chunk != nil {
		chunkSize = jslStep.Chunk.ItemCount
	}

	// Create an instance of JSLAdaptedStep which implements core.Step
	return NewJSLAdaptedStep(jslStep.ID, r, p, w, chunkSize), nil
}
