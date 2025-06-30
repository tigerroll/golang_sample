package jsl

import (
	"embed"
	"fmt"
	"path/filepath"
	"reflect"

	"gopkg.in/yaml.v3"

	"sample/src/main/go/batch/job/core"
	config "sample/src/main/go/batch/config"
	repository "sample/src/main/go/batch/repository"
	stepProcessor "sample/src/main/go/batch/step/processor"
	stepReader "sample/src/main/go/batch/step/reader"
	step "sample/src/main/go/batch/step"
	stepListener "sample/src/main/go/batch/step/listener"
	stepWriter "sample/src/main/go/batch/step/writer"
	exception "sample/src/main/go/batch/util/exception"
	logger "sample/src/main/go/batch/util/logger"
)

// LoadedJobDefinitions holds all loaded JSL job definitions.
var LoadedJobDefinitions = make(map[string]Job)

// LoadJSLDefinitions loads all JSL YAML files embedded in the binary.
func LoadJSLDefinitions(jslFiles embed.FS, subPath string) error { // ★ 修正: subPath を追加
	logger.Infof("JSL 定義のロードを開始します。")
	files, err := jslFiles.ReadDir(subPath) // ★ 修正: subPath を使用
	if err != nil {
		return exception.NewBatchError("jsl_loader", "JSL ディレクトリの読み込みに失敗しました", err, false, false)
	}

	for _, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".yaml" {
			continue
		}

		filePath := filepath.Join(subPath, file.Name()) // ★ 修正: subPath とファイル名を結合
		logger.Debugf("JSL ファイルを読み込み中: %s", filePath)
		data, err := jslFiles.ReadFile(filePath) // ★ 修正: 結合したパスを使用
		if err != nil {
			return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ファイル '%s' の読み込みに失敗しました", filePath), err, false, false)
		}

		var jobDef Job
		if err := yaml.Unmarshal(data, &jobDef); err != nil {
			return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ファイル '%s' のパースに失敗しました", filePath), err, false, false)
		}

		if jobDef.ID == "" {
			return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ファイル '%s' に 'id' が定義されていません", filePath), nil, false, false)
		}
		// ★ 追加: Job.Name のバリデーション
		if jobDef.Name == "" {
			return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ジョブ '%s' に 'name' が定義されていません", jobDef.ID), nil, false, false)
		}
		// ★ 追加: Flow.StartElement のバリデーション
		if jobDef.Flow.StartElement == "" {
			return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ジョブ '%s' のフローに 'start-element' が定義されていません", jobDef.ID), nil, false, false)
		}
		// ★ 追加: Flow.Elements のバリデーション
		if len(jobDef.Flow.Elements) == 0 {
			return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ジョブ '%s' のフローに 'elements' が定義されていません", jobDef.ID), nil, false, false)
		}

		if _, exists := LoadedJobDefinitions[jobDef.ID]; exists {
			return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ジョブID '%s' が重複しています", jobDef.ID), nil, false, false)
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

			isChunk := jslStep.Reader.Ref != "" || jslStep.Processor.Ref != "" || jslStep.Writer.Ref != "" || jslStep.Chunk != nil
			isTasklet := jslStep.Tasklet.Ref != ""

			if isChunk && isTasklet {
				return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("ステップ '%s' はチャンク指向と Tasklet 指向の両方を定義しています。どちらか一方のみを指定してください。", id), nil, false, false)
			}
			if !isChunk && !isTasklet {
				return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("ステップ '%s' はチャンク指向または Tasklet 指向のいずれかの定義が必要です。", id), nil, false, false)
			}

			var coreStep core.Step
			if isChunk {
				// ★ 追加: チャンクステップの必須フィールドと値のバリデーション
				if jslStep.Reader.Ref == "" {
					return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("チャンクステップ '%s' に 'reader' が定義されていません", id), nil, false, false)
				}
				if jslStep.Writer.Ref == "" {
					return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("チャンクステップ '%s' に 'writer' が定義されていません", id), nil, false, false)
				}
				if jslStep.Chunk == nil {
					return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("チャンクステップ '%s' に 'chunk' 設定が定義されていません", id), nil, false, false)
				}
				if jslStep.Chunk.ItemCount <= 0 {
					return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("チャンクステップ '%s' の 'chunk.item-count' は0より大きい値である必要があります", id), nil, false, false)
				}
				if jslStep.Chunk.CommitInterval <= 0 {
					return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("チャンクステップ '%s' の 'chunk.commit-interval' は0より大きい値である必要があります", id), nil, false, false)
				}

				coreStep, err = convertJSLChunkStepToCoreStep(jslStep, componentRegistry, jobRepository, retryConfig, itemRetryConfig, itemSkipConfig, stepListeners, itemReadListeners, itemProcessListeners, itemWriteListeners, skipListeners, retryItemListeners)
			} else { // isTasklet
				// ★ 追加: Taskletステップの必須フィールドのバリデーション
				if jslStep.Tasklet.Ref == "" {
					return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("Taskletステップ '%s' に 'tasklet' が定義されていません", id), nil, false, false)
				}
				coreStep, err = convertJSLTaskletStepToCoreStep(jslStep, componentRegistry, jobRepository, stepListeners)
			}

			if err != nil {
				return nil, err
			}
			coreFlow.Elements[id] = coreStep
			// Add transitions for this step
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
			// Add transitions for this decision
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

// ★ 追加: validateTransition ヘルパー関数
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

// convertJSLChunkStepToCoreStep converts a JSL Step definition to a concrete core.Step implementation for chunk processing.
func convertJSLChunkStepToCoreStep(jslStep Step, componentRegistry map[string]any, jobRepository repository.JobRepository, retryConfig *config.RetryConfig, itemRetryConfig config.ItemRetryConfig, itemSkipConfig config.ItemSkipConfig, stepListeners []stepListener.StepExecutionListener, itemReadListeners []core.ItemReadListener, itemProcessListeners []core.ItemProcessListener, itemWriteListeners []core.ItemWriteListener, skipListeners []stepListener.SkipListener, retryItemListeners []stepListener.RetryItemListener) (core.Step, error) {
	r, ok := componentRegistry[jslStep.Reader.Ref].(stepReader.Reader[any])
	if !ok {
		return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("リーダー '%s' が見つからないか、不正な型です (期待: Reader[any])", jslStep.Reader.Ref), nil, false, false)
	}

	var p stepProcessor.Processor[any, any]
	if jslStep.Processor.Ref != "" {
		p, ok = componentRegistry[jslStep.Processor.Ref].(stepProcessor.Processor[any, any])
		if !ok {
			return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("プロセッサー '%s' が見つからないか、不正な型です (期待: Processor[any, any])", jslStep.Processor.Ref), nil, false, false)
		}
	} else {
		p = nil
	}

	w, ok := componentRegistry[jslStep.Writer.Ref].(stepWriter.Writer[any])
	if !ok {
		return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("ライター '%s' が見つからないか、不正な型です (期待: Writer[any])", jslStep.Writer.Ref), nil, false, false)
	}

	chunkSize := 1
	if jslStep.Chunk != nil {
		chunkSize = jslStep.Chunk.ItemCount
	}

	return step.NewJSLAdaptedStep(jslStep.ID, r, p, w, chunkSize, retryConfig, itemRetryConfig, itemSkipConfig, jobRepository, stepListeners, itemReadListeners, itemProcessListeners, itemWriteListeners, skipListeners, retryItemListeners), nil
}

// convertJSLTaskletStepToCoreStep converts a JSL Step definition to a concrete core.Step implementation for tasklet processing.
func convertJSLTaskletStepToCoreStep(jslStep Step, componentRegistry map[string]any, jobRepository repository.JobRepository, stepListeners []stepListener.StepExecutionListener) (core.Step, error) {
	t, ok := componentRegistry[jslStep.Tasklet.Ref].(step.Tasklet)
	if !ok {
		return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("タスクレット '%s' が見つからないか、不正な型です (期待: Tasklet)", jslStep.Tasklet.Ref), nil, false, false)
	}
	return step.NewTaskletStep(jslStep.ID, t, jobRepository, stepListeners), nil
}
