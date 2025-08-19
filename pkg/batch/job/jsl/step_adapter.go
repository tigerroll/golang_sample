package jsl

import (
	"fmt"
	"reflect"

	config "sample/pkg/batch/config"
	core "sample/pkg/batch/job/core" // core パッケージをインポート
	component "sample/pkg/batch/job/component"
	"sample/pkg/batch/repository/job" // job リポジトリインターフェースをインポート
	step "sample/pkg/batch/step" // そのまま
	logger "sample/pkg/batch/util/logger"
	exception "sample/pkg/batch/util/exception"
	yaml "gopkg.in/yaml.v3"
)

// ConvertJSLToCoreFlow は JSL の Flow 定義を core.FlowDefinition に変換します。
// componentBuilders は、JSLで参照されるコンポーネント（Reader, Processor, Writer, Tasklet）のビルダを保持するマップです。
// jobRepository は、ステップ内でリポジトリが必要な場合に使用されます。
// cfg はアプリケーション全体のコンフィグです。
// listenerBuilders は、ステップレベルおよびアイテムレベルのリスナーを動的に構築するためのマップです。
func ConvertJSLToCoreFlow(
	jslFlow Flow,
	componentBuilders map[string]component.ComponentBuilder, // ★ 変更: component.ComponentBuilder を参照
	jobRepository job.JobRepository, // job.JobRepository に変更
	cfg *config.Config,
	stepListenerBuilders map[string]any, // map[string]StepExecutionListenerBuilder
	itemReadListenerBuilders map[string]any, // map[string]core.ItemReadListenerBuilder
	itemProcessListenerBuilders map[string]any, // map[string]core.ItemProcessListenerBuilder
	itemWriteListenerBuilders map[string]any, // map[string]core.ItemWriteListenerBuilder
	skipListenerBuilders map[string]any, // map[string]stepListener.SkipListenerBuilder
	retryItemListenerBuilders map[string]any, // map[string]stepListener.RetryItemListenerBuilder
) (*core.FlowDefinition, error) { // ★ 変更: db *sql.DB を削除
	module := "jsl_converter"
	flowDef := core.NewFlowDefinition(jslFlow.StartElement)

	// ★ 追加: start-element が elements に存在するかチェック
	if _, ok := jslFlow.Elements[jslFlow.StartElement]; !ok {
		return nil, exception.NewBatchError(module, fmt.Sprintf("フローの 'start-element' '%s' が 'elements' に見つかりません", jslFlow.StartElement), nil, false, false)
	}

	for id, element := range jslFlow.Elements {
		// interface{} から具体的な JSL 構造体 (Step または Decision) に変換するために、
		// 一度 YAML にマーシャルし、再度アンマーシャルする。
		// これは、YAML unmarshalling が interface{} にマップする際の一般的なパターン。
		elementBytes, err := yaml.Marshal(element)
		if err != nil {
			return nil, exception.NewBatchError(module, fmt.Sprintf("フロー要素 '%s' の再マーシャルに失敗しました", id), err, false, false)
		}

		// Step としてアンマーシャルを試みる
		var jslStep Step
		if err := yaml.Unmarshal(elementBytes, &jslStep); err == nil && (jslStep.Reader.Ref != "" || jslStep.Tasklet.Ref != "") {
			// Step であると判断
			jslStep.ID = id // マップのキーからIDを設定

			// ★ 追加: ステップIDがマップのキーと一致するかチェック
			if jslStep.ID != id {
				return nil, exception.NewBatchError(module, fmt.Sprintf("ステップ '%s' のIDがマップのキー '%s' と一致しません", jslStep.ID, id), nil, false, false)
			}

			var coreStep core.Step
			if jslStep.Chunk != nil {
				// チャンク指向ステップ
				if jslStep.Reader.Ref == "" || jslStep.Processor.Ref == "" || jslStep.Writer.Ref == "" {
					return nil, exception.NewBatchErrorf(module, "チャンクステップ '%s' には reader, processor, writer が全て必要です", id)
				}

				// ★ 変更: コンポーネントビルダからコンポーネントをインスタンス化し、プロパティを渡す
				readerBuilder, ok := componentBuilders[jslStep.Reader.Ref]
				if !ok {
					return nil, exception.NewBatchErrorf(module, "リーダー '%s' のビルダーが見つかりません", jslStep.Reader.Ref)
				}
				readerInstance, err := readerBuilder(cfg, jobRepository, jslStep.Reader.Properties)
				if err != nil {
					return nil, exception.NewBatchError(module, fmt.Sprintf("リーダー '%s' のビルドに失敗しました", jslStep.Reader.Ref), err, false, false)
				}
				r, isReader := readerInstance.(core.ItemReader[any])
				if !isReader {
					return nil, exception.NewBatchErrorf(module, "リーダー '%s' が見つからないか、不正な型です (期待: ItemReader[any], 実際: %s)", jslStep.Reader.Ref, reflect.TypeOf(readerInstance))
				}

				processorBuilder, ok := componentBuilders[jslStep.Processor.Ref]
				if !ok {
					return nil, exception.NewBatchErrorf(module, "プロセッサ '%s' のビルダーが見つかりません", jslStep.Processor.Ref)
				}
				processorInstance, err := processorBuilder(cfg, jobRepository, jslStep.Processor.Properties)
				if err != nil {
					return nil, exception.NewBatchError(module, fmt.Sprintf("プロセッサ '%s' のビルドに失敗しました", jslStep.Processor.Ref), err, false, false)
				}
				p, isProcessor := processorInstance.(core.ItemProcessor[any, any])
				if !isProcessor {
					return nil, exception.NewBatchErrorf(module, "プロセッサ '%s' が見つからないか、不正な型です (期待: ItemProcessor[any, any], 実際: %s)", jslStep.Processor.Ref, reflect.TypeOf(processorInstance))
				}

				writerBuilder, ok := componentBuilders[jslStep.Writer.Ref]
				if !ok {
					return nil, exception.NewBatchErrorf(module, "ライター '%s' のビルダーが見つかりません", jslStep.Writer.Ref)
				}
				writerInstance, err := writerBuilder(cfg, jobRepository, jslStep.Writer.Properties)
				if err != nil {
					return nil, exception.NewBatchError(module, fmt.Sprintf("ライター '%s' のビルドに失敗しました", jslStep.Writer.Ref), err, false, false)
				}
				w, isWriter := writerInstance.(core.ItemWriter[any])
				if !isWriter {
					return nil, exception.NewBatchErrorf(module, "ライター '%s' が見つからないか、不正な型です (期待: ItemWriter[any], 実際: %s)", jslStep.Writer.Ref, reflect.TypeOf(writerInstance))
				}

				// ステップレベルリスナーの構築
				var stepExecListeners []core.StepExecutionListener
				for _, listenerRef := range jslStep.Listeners {
					builderAny, found := stepListenerBuilders[listenerRef.Ref]
					if !found {
						return nil, exception.NewBatchErrorf(module, "StepExecutionListener '%s' のビルダーが登録されていません", listenerRef.Ref)
					}
					builder, ok := builderAny.(func(*config.Config) (core.StepExecutionListener, error))
					if !ok {
						return nil, exception.NewBatchErrorf(module, "StepExecutionListener ビルダー '%s' の型が不正です (期待: func(*config.Config) (core.StepExecutionListener, error), 実際: %s)", listenerRef.Ref, reflect.TypeOf(builderAny))
					}
					listenerInstance, err := builder(cfg)
					if err != nil {
						return nil, exception.NewBatchError(module, fmt.Sprintf("StepExecutionListener '%s' のビルドに失敗しました", listenerRef.Ref), err, false, false)
					}
					stepExecListeners = append(stepExecListeners, listenerInstance)
				}

				// アイテムレベルリスナーの構築
				var itemReadListeners []core.ItemReadListener
				for _, listenerRef := range jslStep.ItemReadListeners {
					builderAny, found := itemReadListenerBuilders[listenerRef.Ref]
					if !found {
						return nil, exception.NewBatchErrorf(module, "ItemReadListener '%s' のビルダーが登録されていません", listenerRef.Ref)
					}
					builder, ok := builderAny.(func(*config.Config) (core.ItemReadListener, error))
					if !ok {
						return nil, exception.NewBatchErrorf(module, "ItemReadListener ビルダー '%s' の型が不正です (期待: func(*config.Config) (core.ItemReadListener, error), 実際: %s)", listenerRef.Ref, reflect.TypeOf(builderAny))
					}
					listenerInstance, err := builder(cfg)
					if err != nil {
						return nil, exception.NewBatchError(module, fmt.Sprintf("ItemReadListener '%s' のビルドに失敗しました", listenerRef.Ref), err, false, false)
					}
					itemReadListeners = append(itemReadListeners, listenerInstance)
				}

				var itemProcessListeners []core.ItemProcessListener
				for _, listenerRef := range jslStep.ItemProcessListeners {
					builderAny, found := itemProcessListenerBuilders[listenerRef.Ref]
					if !found {
						return nil, exception.NewBatchErrorf(module, "ItemProcessListener '%s' のビルダーが登録されていません", listenerRef.Ref)
					}
					builder, ok := builderAny.(func(*config.Config) (core.ItemProcessListener, error))
					if !ok {
						return nil, exception.NewBatchErrorf(module, "ItemProcessListener ビルダー '%s' の型が不正です (期待: func(*config.Config) (core.ItemProcessListener, error), 実際: %s)", listenerRef.Ref, reflect.TypeOf(builderAny))
					}
					listenerInstance, err := builder(cfg)
					if err != nil {
						return nil, exception.NewBatchError(module, fmt.Sprintf("ItemProcessListener '%s' のビルドに失敗しました", listenerRef.Ref), err, false, false)
					}
					itemProcessListeners = append(itemProcessListeners, listenerInstance)
				}

				var itemWriteListeners []core.ItemWriteListener
				for _, listenerRef := range jslStep.ItemWriteListeners {
					builderAny, found := itemWriteListenerBuilders[listenerRef.Ref]
					if !found {
						return nil, exception.NewBatchErrorf(module, "ItemWriteListener '%s' のビルダーが登録されていません", listenerRef.Ref)
					}
					builder, ok := builderAny.(func(*config.Config) (core.ItemWriteListener, error))
					if !ok {
						return nil, exception.NewBatchErrorf(module, "ItemWriteListener ビルダー '%s' の型が不正です (期待: func(*config.Config) (core.ItemWriteListener, error), 実際: %s)", listenerRef.Ref, reflect.TypeOf(builderAny))
					}
					listenerInstance, err := builder(cfg)
					if err != nil {
						return nil, exception.NewBatchError(module, fmt.Sprintf("ItemWriteListener '%s' のビルドに失敗しました", listenerRef.Ref), err, false, false)
					}
					itemWriteListeners = append(itemWriteListeners, listenerInstance)
				}

				var skipListeners []core.SkipListener
				for _, listenerRef := range jslStep.SkipListeners {
					builderAny, found := skipListenerBuilders[listenerRef.Ref]
					if !found {
						return nil, exception.NewBatchErrorf(module, "SkipListener '%s' のビルダーが登録されていません", listenerRef.Ref)
					}
					builder, ok := builderAny.(func(*config.Config) (core.SkipListener, error))
					if !ok {
						return nil, exception.NewBatchErrorf(module, "SkipListener ビルダー '%s' の型が不正です (期待: func(*config.Config) (core.SkipListener, error), 実際: %s)", listenerRef.Ref, reflect.TypeOf(builderAny))
					}
					listenerInstance, err := builder(cfg)
					if err != nil {
						return nil, exception.NewBatchError(module, fmt.Sprintf("SkipListener '%s' のビルドに失敗しました", listenerRef.Ref), err, false, false)
					}
					skipListeners = append(skipListeners, listenerInstance)
				}

				var retryItemListeners []core.RetryItemListener
				for _, listenerRef := range jslStep.RetryItemListeners {
					builderAny, found := retryItemListenerBuilders[listenerRef.Ref]
					if !found {
						return nil, exception.NewBatchErrorf(module, "RetryItemListener '%s' のビルダーが登録されていません", listenerRef.Ref)
					}
					builder, ok := builderAny.(func(*config.Config) (core.RetryItemListener, error))
					if !ok {
						return nil, exception.NewBatchErrorf(module, "RetryItemListener ビルダー '%s' の型が不正です (期待: func(*config.Config) (core.RetryItemListener, error), 実際: %s)", listenerRef.Ref, reflect.TypeOf(builderAny))
					}
					listenerInstance, err := builder(cfg)
					if err != nil {
						return nil, exception.NewBatchError(module, fmt.Sprintf("RetryItemListener '%s' のビルドに失敗しました", listenerRef.Ref), err, false, false)
					}
					retryItemListeners = append(retryItemListeners, listenerInstance)
				}

				coreStep = step.NewJSLAdaptedStep(
					jslStep.ID, // name
					r,
					p,
					w,
					jslStep.Chunk.ItemCount, // chunkSize
					&cfg.Batch.Retry, // stepRetryConfig
					cfg.Batch.ItemRetry, // itemRetryCfg
					cfg.Batch.ItemSkip,  // itemSkipCfg
					jobRepository, // repo
					stepExecListeners,
					itemReadListeners,
					itemProcessListeners,
					itemWriteListeners,
					skipListeners,
					retryItemListeners,
				)
				logger.Debugf("チャンクステップ '%s' を構築しました。", id)

			} else if jslStep.Tasklet.Ref != "" {
				// タスクレット指向ステップ
				taskletBuilder, ok := componentBuilders[jslStep.Tasklet.Ref] // ★ 変更: componentRegistry から componentBuilders に変更
				if !ok {
					return nil, exception.NewBatchErrorf(module, "タスクレット '%s' のビルダーが見つかりません", jslStep.Tasklet.Ref)
				}
				taskletInstance, err := taskletBuilder(cfg, jobRepository, jslStep.Tasklet.Properties) // ★ 変更: properties を渡す
				if err != nil {
					return nil, exception.NewBatchError(module, fmt.Sprintf("タスクレット '%s' のビルドに失敗しました", jslStep.Tasklet.Ref), err, false, false)
				}
				t, isTasklet := taskletInstance.(core.Tasklet) // step.Tasklet インターフェースに型アサーション
				if !isTasklet {
					return nil, exception.NewBatchErrorf(module, "タスクレット '%s' が見つからないか、不正な型です (期待: core.Tasklet, 実際: %s)", jslStep.Tasklet.Ref, reflect.TypeOf(taskletInstance))
				}

				// ステップレベルリスナーの構築 (チャンクステップと同じ)
				var stepExecListeners []core.StepExecutionListener
				for _, listenerRef := range jslStep.Listeners {
					builderAny, found := stepListenerBuilders[listenerRef.Ref]
					if !found {
						return nil, exception.NewBatchErrorf(module, "StepExecutionListener '%s' のビルダーが登録されていません", listenerRef.Ref)
					}
					builder, ok := builderAny.(func(*config.Config) (core.StepExecutionListener, error))
					if !ok {
						return nil, exception.NewBatchErrorf(module, "StepExecutionListener ビルダー '%s' の型が不正です (期待: func(*config.Config) (core.StepExecutionListener, error), 実際: %s)", listenerRef.Ref, reflect.TypeOf(builderAny))
					}
					listenerInstance, err := builder(cfg)
					if err != nil {
						return nil, exception.NewBatchError(module, fmt.Sprintf("StepExecutionListener '%s' のビルドに失敗しました", listenerRef.Ref), err, false, false)
					}
					stepExecListeners = append(stepExecListeners, listenerInstance)
				}

				coreStep = step.NewTaskletStep(
					jslStep.ID, // name
					t,
					jobRepository, // JobRepository を渡す
					stepExecListeners,
				)
				logger.Debugf("タスクレットステップ '%s' を構築しました。", id)

			} else {
				return nil, exception.NewBatchErrorf(module, "ステップ '%s' はチャンクまたはタスクレットのいずれかを定義する必要があります", id)
			}

			err = flowDef.AddElement(id, coreStep)
			if err != nil {
				return nil, exception.NewBatchError(module, fmt.Sprintf("フローにステップ '%s' の追加に失敗しました", id), err, false, false)
			}

			// ステップの遷移ルールを追加
			for _, transition := range jslStep.Transitions {
				// ★ 追加: 遷移ルールのバリデーション
				if err := validateTransition(id, transition, jslFlow.Elements); err != nil {
					return nil, err
				}
				flowDef.AddTransitionRule(id, transition.On, transition.To, transition.End, transition.Fail, transition.Stop)
			}

		} else {
			// Decision としてアンマーシャルを試みる
			var jslDecision Decision
			if err := yaml.Unmarshal(elementBytes, &jslDecision); err == nil && jslDecision.ID != "" { // len(jslDecision.Transitions) > 0 は Decision に遷移ルールが必須であることを確認するため、後で追加
				// ★ 追加: デシジョンIDがマップのキーと一致するかチェック
				if jslDecision.ID != id {
					return nil, exception.NewBatchError(module, fmt.Sprintf("デシジョン '%s' のIDがマップのキー '%s' と一致しません", jslDecision.ID, id), nil, false, false)
				}
				// ★ 追加: デシジョンに遷移ルールが必須であることを確認
				if len(jslDecision.Transitions) == 0 {
					return nil, exception.NewBatchError(module, fmt.Sprintf("デシジョン '%s' に遷移ルールが定義されていません", id), nil, false, false)
				}

				coreDecision := core.NewSimpleDecision(jslDecision.ID) // SimpleDecision を使用
				err := flowDef.AddElement(id, coreDecision)
				if err != nil {
					return nil, exception.NewBatchError(module, fmt.Sprintf("フローに Decision '%s' の追加に失敗しました", id), err, false, false)
				}

				// Decision の遷移ルールを追加
				for _, transition := range jslDecision.Transitions {
					// ★ 追加: 遷移ルールのバリデーション
					if err := validateTransition(id, transition, jslFlow.Elements); err != nil {
						return nil, err
					}
					flowDef.AddTransitionRule(id, transition.On, transition.To, transition.End, transition.Fail, transition.Stop)
				}
				logger.Debugf("Decision '%s' を構築しました。", id)

			} else {
				return nil, exception.NewBatchErrorf(module, "不明なフロー要素の型です: ID '%s', データ: %s", id, string(elementBytes))
			}
		}
	}

	return flowDef, nil
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
		// ★ 修正: ここでエラーオブジェクトのみを返すように変更
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
