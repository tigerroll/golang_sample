package jsl

import (
	"fmt"
	"reflect"

	"gopkg.in/yaml.v3"

	"sample/pkg/batch/config"
	"sample/pkg/batch/job/core"
	repository "sample/pkg/batch/repository"
	stepListener "sample/pkg/batch/step/listener"
	exception "sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"
)

// LoadedJobDefinitions holds all loaded JSL job definitions.
// このマップは、JobFactory がジョブIDで定義を取得するために必要です。
var LoadedJobDefinitions = make(map[string]Job)

// LoadJSLDefinitionFromBytes は単一のJSL YAMLファイルのバイトデータからジョブ定義をロードします。
// この関数はアプリケーション側で埋め込まれた単一のJSLファイルをロードするために使用されます。
func LoadJSLDefinitionFromBytes(data []byte) error {
	logger.Infof("JSL 定義のロードを開始します。")

	var jobDef Job
	if err := yaml.Unmarshal(data, &jobDef); err != nil {
		return exception.NewBatchError("jsl_loader", "JSL ファイルのパースに失敗しました", err, false, false)
	}

	if jobDef.ID == "" {
		return exception.NewBatchError("jsl_loader", "JSL ファイルに 'id' が定義されていません", nil, false, false)
	}
	if jobDef.Name == "" {
		return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ジョブ '%s' に 'name' が定義されていません", jobDef.ID), nil, false, false)
	}
	if jobDef.Flow.StartElement == "" {
		return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ジョブ '%s' のフローに 'start-element' が定義されていません", jobDef.ID), nil, false, false)
	}
	if len(jobDef.Flow.Elements) == 0 {
		return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ジョブ '%s' のフローに 'elements' が定義されていません", jobDef.ID), nil, false, false)
	}

	if _, exists := LoadedJobDefinitions[jobDef.ID]; exists {
		return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ジョブID '%s' が重複しています", jobDef.ID), nil, false, false)
	}

	LoadedJobDefinitions[jobDef.ID] = jobDef
	logger.Infof("JSL ジョブ '%s' をロードしました。", jobDef.ID)
	logger.Infof("JSL 定義のロードが完了しました。ロードされたジョブ数: %d", len(LoadedJobDefinitions))
	return nil
}

// GetJobDefinition retrieves a JSL Job definition by its ID.
func GetJobDefinition(jobID string) (Job, bool) {
	job, ok := LoadedJobDefinitions[jobID]
	return job, ok
}

// GetLoadedJobCount returns the number of loaded JSL job definitions.
func GetLoadedJobCount() int {
	return len(LoadedJobDefinitions)
}

// ConvertJSLToCoreFlow converts a JSL Flow definition into a core.FlowDefinition.
func ConvertJSLToCoreFlow(jslFlow Flow, componentRegistry map[string]any, jobRepository repository.JobRepository, retryConfig *config.RetryConfig, itemRetryConfig config.ItemRetryConfig, itemSkipConfig config.ItemSkipConfig, stepListeners []stepListener.StepExecutionListener, itemReadListeners []core.ItemReadListener, itemProcessListeners []core.ItemProcessListener, itemWriteListeners []core.ItemWriteListener, skipListeners []stepListener.SkipListener, retryItemListeners []stepListener.RetryItemListener) (*core.FlowDefinition, error) {
	coreFlow := &core.FlowDefinition{
		StartElement:    jslFlow.StartElement,
		Elements:        make(map[string]core.FlowElement),
		TransitionRules: make([]core.TransitionRule, 0),
	}

	// ★ 追加: start-element が elements に存在するかチェック
	if _, ok := jslFlow.Elements[jslFlow.StartElement]; !ok {
		return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("フローの 'start-element' '%s' が 'elements' に見つかりません", jslFlow.StartElement), nil, false, false)
	}

	for id, elem := range jslFlow.Elements {
		elemBytes, err := yaml.Marshal(elem)
		if err != nil {
			return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("フロー要素 '%s' の再マーシャルに失敗しました", id), err, false, false)
		}

		// Try to unmarshal as Step
		var jslStep Step
		if err := yaml.Unmarshal(elemBytes, &jslStep); err == nil && jslStep.ID != "" {
			// ★ 追加: ステップIDがマップのキーと一致するかチェック
			if jslStep.ID != id {
				return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("ステップ '%s' のIDがマップのキー '%s' と一致しません", jslStep.ID, id), nil, false, false)
			}

			// 新しいアダプター関数を使用してコアステップを作成
			coreStep, err := NewCoreStep(jslStep, componentRegistry, jobRepository, retryConfig, itemRetryConfig, itemSkipConfig, stepListeners, itemReadListeners, itemProcessListeners, itemWriteListeners, skipListeners, retryItemListeners)
			if err != nil {
				return nil, err
			}
			coreFlow.Elements[id] = coreStep
			// このステップの遷移を追加
			for _, t := range jslStep.Transitions {
				// ★ 追加: 遷移ルールのバリデーション
				if err := validateTransition(id, t, jslFlow.Elements); err != nil {
					return nil, err
				}
				coreFlow.AddTransitionRule(id, t.On, t.To, t.End, t.Fail, t.Stop)
			}
			continue
		}

		// Try to unmarshal as Decision
		var jslDecision Decision
		if err := yaml.Unmarshal(elemBytes, &jslDecision); err == nil && jslDecision.ID != "" { // len(jslDecision.Transitions) > 0 は Decision に遷移ルールが必須であることを確認するため、後で追加
			// ★ 追加: デシジョンIDがマップのキーと一致するかチェック
			if jslDecision.ID != id {
				return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("デシジョン '%s' のIDがマップのキー '%s' と一致しません", jslDecision.ID, id), nil, false, false)
			}
			// ★ 追加: デシジョンに遷移ルールが必須であることを確認
			if len(jslDecision.Transitions) == 0 {
				return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("デシジョン '%s' に遷移ルールが定義されていません", id), nil, false, false)
			}

			coreDecision := core.NewSimpleDecision(jslDecision.ID)
			coreFlow.Elements[id] = coreDecision
			// このデシジョンの遷移を追加
			for _, t := range jslDecision.Transitions {
				// ★ 追加: 遷移ルールのバリデーション
				if err := validateTransition(id, t, jslFlow.Elements); err != nil {
					return nil, err
				}
				coreFlow.AddTransitionRule(id, t.On, t.To, t.End, t.Fail, t.Stop)
			}
			continue
		}

		return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("不明なフロー要素の型または必須フィールドが不足しています (ID: %s, Type: %s)", id, reflect.TypeOf(elem)), nil, false, false)
	}
	return coreFlow, nil
}

// validateTransition validates a single transition rule.
func validateTransition(fromElementID string, t Transition, allElements map[string]interface{}) error {
	if t.On == "" {
		return exception.NewBatchError("jsl_converter", fmt.Sprintf("フロー要素 '%s' の遷移ルールに 'on' が定義されていません", fromElementID), nil, false, false)
	}

	// Check mutual exclusivity of End, Fail, Stop, To
	exclusiveCount := 0
	if t.End {
		exclusiveCount++
	}
	if t.Fail {
		exclusiveCount++
	}
	if t.Stop {
		exclusiveCount++
	}
	if t.To != "" {
		exclusiveCount++
	}

	if exclusiveCount == 0 {
		return exception.NewBatchError("jsl_converter", fmt.Sprintf("フロー要素 '%s' の遷移ルール (on: '%s') に 'to', 'end', 'fail', 'stop' のいずれも定義されていません", fromElementID, t.On), nil, false, false)
	}
	if exclusiveCount > 1 {
		return exception.NewBatchError("jsl_converter", fmt.Sprintf("フロー要素 '%s' の遷移ルール (on: '%s') は 'to', 'end', 'fail', 'stop' のうち複数定義されています。これらは排他的です。", fromElementID, t.On), nil, false, false)
	}

	// If 'to' is specified, ensure the target element exists
	if t.To != "" {
		if _, ok := allElements[t.To]; !ok {
			return exception.NewBatchError("jsl_converter", fmt.Sprintf("フロー要素 '%s' の遷移ルール (on: '%s') の 'to' で指定された要素 '%s' が見つかりません", fromElementID, t.On, t.To), nil, false, false)
		}
	}
	return nil
}
