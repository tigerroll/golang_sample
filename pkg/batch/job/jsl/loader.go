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
func ConvertJSLToCoreFlow(
	jslFlow Flow,
	componentRegistry map[string]any,
	jobRepository repository.JobRepository,
	cfg *config.Config, // Config 全体を渡す
	stepListenerBuilders map[string]any, // 型を any に変更
	itemReadListenerBuilders map[string]any, // 型を any に変更
	itemProcessListenerBuilders map[string]any, // 型を any に変更
	itemWriteListenerBuilders map[string]any, // 型を any に変更
	skipListenerBuilders map[string]any, // 型を any に変更
	retryItemListenerBuilders map[string]any, // 型を any に変更
) (*core.FlowDefinition, error) {
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

			// ステップレベルリスナーのインスタンス化
			var stepLs []stepListener.StepExecutionListener
			for _, listenerRef := range jslStep.Listeners {
				builderAny, found := stepListenerBuilders[listenerRef.Ref]
				if !found {
					return nil, exception.NewBatchErrorf("jsl_converter", "StepExecutionListener '%s' のビルダーが登録されていません", listenerRef.Ref)
				}
				builder, ok := builderAny.(func(*config.Config) (stepListener.StepExecutionListener, error)) // 型アサーション
				if !ok {
					return nil, exception.NewBatchErrorf("jsl_converter", "StepExecutionListener '%s' のビルダーの型が不正です: %T", listenerRef.Ref, builderAny)
				}
				listenerInstance, err := builder(cfg)
				if err != nil {
					return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("StepExecutionListener '%s' のビルドに失敗しました", listenerRef.Ref), err, false, false)
				}
				stepLs = append(stepLs, listenerInstance)
			}

			// アイテムレベルリスナーのインスタンス化 (チャンクステップのみに適用されるが、ここでは全てインスタンス化)
			var itemReadLs []core.ItemReadListener
			for _, listenerRef := range jslStep.ItemReadListeners {
				builderAny, found := itemReadListenerBuilders[listenerRef.Ref]
				if !found {
					return nil, exception.NewBatchErrorf("jsl_converter", "ItemReadListener '%s' のビルダーが登録されていません", listenerRef.Ref)
				}
				builder, ok := builderAny.(func(*config.Config) (core.ItemReadListener, error)) // 型アサーション
				if !ok {
					return nil, exception.NewBatchErrorf("jsl_converter", "ItemReadListener '%s' のビルダーの型が不正です: %T", listenerRef.Ref, builderAny)
				}
				listenerInstance, err := builder(cfg)
				if err != nil {
					return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("ItemReadListener '%s' のビルドに失敗しました", listenerRef.Ref), err, false, false)
				}
				itemReadLs = append(itemReadLs, listenerInstance)
			}
			var itemProcessLs []core.ItemProcessListener
			for _, listenerRef := range jslStep.ItemProcessListeners {
				builderAny, found := itemProcessListenerBuilders[listenerRef.Ref]
				if !found {
					return nil, exception.NewBatchErrorf("jsl_converter", "ItemProcessListener '%s' のビルダーが登録されていません", listenerRef.Ref)
				}
				builder, ok := builderAny.(func(*config.Config) (core.ItemProcessListener, error)) // 型アサーション
				if !ok {
					return nil, exception.NewBatchErrorf("jsl_converter", "ItemProcessListener '%s' のビルダーの型が不正です: %T", listenerRef.Ref, builderAny)
				}
				listenerInstance, err := builder(cfg)
				if err != nil {
					return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("ItemProcessListener '%s' のビルドに失敗しました", listenerRef.Ref), err, false, false)
				}
				itemProcessLs = append(itemProcessLs, listenerInstance)
			}
			var itemWriteLs []core.ItemWriteListener
			for _, listenerRef := range jslStep.ItemWriteListeners {
				builderAny, found := itemWriteListenerBuilders[listenerRef.Ref]
				if !found {
					return nil, exception.NewBatchErrorf("jsl_converter", "ItemWriteListener '%s' のビルダーが登録されていません", listenerRef.Ref)
				}
				builder, ok := builderAny.(func(*config.Config) (core.ItemWriteListener, error)) // 型アサーション
				if !ok {
					return nil, exception.NewBatchErrorf("jsl_converter", "ItemWriteListener '%s' のビルダーの型が不正です: %T", listenerRef.Ref, builderAny)
				}
				listenerInstance, err := builder(cfg)
				if err != nil {
					return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("ItemWriteListener '%s' のビルドに失敗しました", listenerRef.Ref), err, false, false)
				}
				itemWriteLs = append(itemWriteLs, listenerInstance)
			}
			var skipLs []stepListener.SkipListener
			for _, listenerRef := range jslStep.SkipListeners {
				builderAny, found := skipListenerBuilders[listenerRef.Ref]
				if !found {
					return nil, exception.NewBatchErrorf("jsl_converter", "SkipListener '%s' のビルダーが登録されていません", listenerRef.Ref)
				}
				builder, ok := builderAny.(func(*config.Config) (stepListener.SkipListener, error)) // 型アサーション
				if !ok {
					return nil, exception.NewBatchErrorf("jsl_converter", "SkipListener '%s' のビルダーの型が不正です: %T", listenerRef.Ref, builderAny)
				}
				listenerInstance, err := builder(cfg)
				if err != nil {
					return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("SkipListener '%s' のビルドに失敗しました", listenerRef.Ref), err, false, false)
				}
				skipLs = append(skipLs, listenerInstance)
			}
			var retryItemLs []stepListener.RetryItemListener
			for _, listenerRef := range jslStep.RetryItemListeners {
				builderAny, found := retryItemListenerBuilders[listenerRef.Ref]
				if !found {
					return nil, exception.NewBatchErrorf("jsl_converter", "RetryItemListener '%s' のビルダーが登録されていません", listenerRef.Ref)
				}
				builder, ok := builderAny.(func(*config.Config) (stepListener.RetryItemListener, error)) // 型アサーション
				if !ok {
					return nil, exception.NewBatchErrorf("jsl_converter", "RetryItemListener '%s' のビルダーの型が不正です: %T", listenerRef.Ref, builderAny)
				}
				listenerInstance, err := builder(cfg)
				if err != nil {
					return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("RetryItemListener '%s' のビルドに失敗しました", listenerRef.Ref), err, false, false)
				}
				retryItemLs = append(retryItemLs, listenerInstance)
			}

			// 新しいアダプター関数を使用してコアステップを作成
			coreStep, err := NewCoreStep(
				jslStep,
				componentRegistry,
				jobRepository,
				&cfg.Batch.Retry, // Step Retry Config
				cfg.Batch.ItemRetry, // Item Retry Config
				cfg.Batch.ItemSkip, // Item Skip Config
				stepLs,
				itemReadLs,
				itemProcessLs,
				itemWriteLs,
				skipLs,
				retryItemLs,
			)
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

			coreDecision := core.NewSimpleDecision(jslDecision.ID) // Decision doesn't take listeners
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
