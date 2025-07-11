package jsl

import (
	"fmt"

	"sample/pkg/batch/config"
	"sample/pkg/batch/job/core"
	"sample/pkg/batch/repository"
	"sample/pkg/batch/step"
	stepListener "sample/pkg/batch/step/listener"
	stepProcessor "sample/pkg/batch/step/processor"
	stepReader "sample/pkg/batch/step/reader"
	stepWriter "sample/pkg/batch/step/writer"
	"sample/pkg/batch/util/exception"
)

// NewCoreStep は JSL Step 定義を core.Step 実装に変換します。
// チャンク指向ステップとタスクレット指向ステップの両方を扱い、必要なバリデーションと
// componentRegistry からのコンポーネント参照の解決を行います。
func NewCoreStep(
	jslStep Step,
	componentRegistry map[string]any,
	jobRepository repository.JobRepository,
	retryConfig *config.RetryConfig,
	itemRetryConfig config.ItemRetryConfig,
	itemSkipConfig config.ItemSkipConfig,
	stepListeners []stepListener.StepExecutionListener,
	itemReadListeners []core.ItemReadListener,
	itemProcessListeners []core.ItemProcessListener,
	itemWriteListeners []core.ItemWriteListener,
	skipListeners []stepListener.SkipListener,
	retryItemListeners []stepListener.RetryItemListener,
) (core.Step, error) {
	isChunk := jslStep.Reader.Ref != "" || jslStep.Processor.Ref != "" || jslStep.Writer.Ref != "" || jslStep.Chunk != nil
	isTasklet := jslStep.Tasklet.Ref != ""

	if isChunk && isTasklet {
		return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("ステップ '%s' はチャンク指向と Tasklet 指向の両方を定義しています。どちらか一方のみを指定してください。", jslStep.ID), nil, false, false)
	}
	if !isChunk && !isTasklet {
		return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("ステップ '%s' はチャンク指向または Tasklet 指向のいずれかの定義が必要です。", jslStep.ID), nil, false, false)
	}

	if isChunk {
		// チャンクステップのバリデーション
		if jslStep.Reader.Ref == "" {
			return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("チャンクステップ '%s' に 'reader' が定義されていません", jslStep.ID), nil, false, false)
		}
		if jslStep.Writer.Ref == "" {
			return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("チャンクステップ '%s' に 'writer' が定義されていません", jslStep.ID), nil, false, false)
		}
		if jslStep.Chunk == nil {
			return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("チャンクステップ '%s' に 'chunk' 設定が定義されていません", jslStep.ID), nil, false, false)
		}
		if jslStep.Chunk.ItemCount <= 0 {
			return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("チャンクステップ '%s' の 'chunk.item-count' は0より大きい値である必要があります", jslStep.ID), nil, false, false)
		}
		if jslStep.Chunk.CommitInterval <= 0 {
			return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("チャンクステップ '%s' の 'chunk.commit-interval' は0より大きい値である必要があります", jslStep.ID), nil, false, false)
		}
		return convertJSLChunkStepToCoreStep(jslStep, componentRegistry, jobRepository, retryConfig, itemRetryConfig, itemSkipConfig, stepListeners, itemReadListeners, itemProcessListeners, itemWriteListeners, skipListeners, retryItemListeners)
	} else { // isTasklet
		// Taskletステップのバリデーション
		if jslStep.Tasklet.Ref == "" {
			return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("Taskletステップ '%s' に 'tasklet' が定義されていません", jslStep.ID), nil, false, false)
		}
		return convertJSLTaskletStepToCoreStep(jslStep, componentRegistry, jobRepository, stepListeners)
	}
}

// convertJSLChunkStepToCoreStep は JSL Step 定義をチャンク処理用の core.Step 実装に変換します。
// この関数は jsl パッケージ内でプライベートです。
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

// convertJSLTaskletStepToCoreStep は JSL Step 定義をタスクレット処理用の core.Step 実装に変換します。
// この関数は jsl パッケージ内でプライベートです。
func convertJSLTaskletStepToCoreStep(jslStep Step, componentRegistry map[string]any, jobRepository repository.JobRepository, stepListeners []stepListener.StepExecutionListener) (core.Step, error) {
	t, ok := componentRegistry[jslStep.Tasklet.Ref].(step.Tasklet)
	if !ok {
		return nil, exception.NewBatchError("jsl_converter", fmt.Sprintf("タスクレット '%s' が見つからないか、不正な型です (期待: Tasklet)", jslStep.Tasklet.Ref), nil, false, false)
	}
	return step.NewTaskletStep(jslStep.ID, t, jobRepository, stepListeners), nil
}
