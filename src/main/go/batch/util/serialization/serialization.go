package serialization

import (
	"encoding/json"
	"fmt"

	core "sample/src/main/go/batch/job/core"
	"sample/src/main/go/batch/util/exception" // exception パッケージをインポート
	logger "sample/src/main/go/batch/util/logger" // logger パッケージをインポート
)

// MarshalExecutionContext は ExecutionContext を JSON バイトスライスにシリアライズします。
func MarshalExecutionContext(ctx core.ExecutionContext) ([]byte, error) {
	module := "serialization"
	logger.Debugf("ExecutionContext のシリアライズを開始します。")

	if ctx == nil {
		logger.Debugf("ExecutionContext が nil です。空のJSONオブジェクトを返します。")
		return []byte("{}"), nil // nil の場合は空のJSONオブジェクトを返す
	}
	data, err := json.Marshal(ctx)
	if err != nil {
		logger.Errorf("ExecutionContext のシリアライズに失敗しました: %v", err)
		return nil, exception.NewBatchError(module, "ExecutionContext のシリアライズに失敗しました", err, false, false)
	}
	logger.Debugf("ExecutionContext のシリアライズが完了しました。")
	return data, nil
}

// UnmarshalExecutionContext は JSON バイトスライスを ExecutionContext にデシリアライズします。
func UnmarshalExecutionContext(data []byte, ctx *core.ExecutionContext) error {
	module := "serialization"
	logger.Debugf("ExecutionContext のデシリアライズを開始します。データサイズ: %d バイト", len(data))

	if len(data) == 0 || string(data) == "null" {
		if *ctx == nil {
			*ctx = core.NewExecutionContext()
			logger.Debugf("ExecutionContext が nil または空データです。新しい空の ExecutionContext を作成しました。")
		} else {
			for k := range *ctx {
				delete(*ctx, k)
			}
			logger.Debugf("ExecutionContext が空データです。既存の ExecutionContext をクリアしました。")
		}
		return nil
	}

	if *ctx == nil {
		*ctx = core.NewExecutionContext()
		logger.Debugf("ExecutionContext が nil です。デシリアライズ用に新しいマップを作成しました。")
	} else {
		for k := range *ctx {
			delete(*ctx, k)
		}
		logger.Debugf("既存の ExecutionContext をデシリアライズ用にクリアしました。")
	}

	err := json.Unmarshal(data, ctx)
	if err != nil {
		logger.Errorf("ExecutionContext のデシリアライズに失敗しました: %v", err)
		return exception.NewBatchError(module, "ExecutionContext のデシリアライズに失敗しました", err, false, false)
	}
	logger.Debugf("ExecutionContext のデシリアライズが完了しました。")
	return nil
}

// MarshalJobParameters は JobParameters を JSON バイトスライスにシリアライズします。
func MarshalJobParameters(params core.JobParameters) ([]byte, error) {
	module := "serialization"
	logger.Debugf("JobParameters のシリアライズを開始します。")

	if params.Params == nil {
		logger.Debugf("JobParameters.Params が nil です。空のJSONオブジェクトを返します。")
		return []byte("{}"), nil
	}
	data, err := json.Marshal(params.Params)
	if err != nil {
		logger.Errorf("JobParameters のシリアライズに失敗しました: %v", err)
		return nil, exception.NewBatchError(module, "JobParameters のシリアライズに失敗しました", err, false, false)
	}
	logger.Debugf("JobParameters のシリアライズが完了しました。")
	return data, nil
}

// UnmarshalJobParameters は JSON バイトスライスを JobParameters にデシリアライズします。
func UnmarshalJobParameters(data []byte, params *core.JobParameters) error {
	module := "serialization"
	logger.Debugf("JobParameters のデシリアライズを開始します。データサイズ: %d バイト", len(data))

	if len(data) == 0 || string(data) == "null" {
		*params = core.NewJobParameters()
		logger.Debugf("JobParameters が nil または空データです。新しい空の JobParameters を作成しました。")
		return nil
	}

	if params.Params == nil {
		params.Params = make(map[string]interface{})
		logger.Debugf("JobParameters.Params が nil です。デシリアライズ用に新しいマップを作成しました。")
	} else {
		for k := range params.Params {
			delete(params.Params, k)
		}
		logger.Debugf("既存の JobParameters.Params をデシリアライズ用にクリアしました。")
	}

	err := json.Unmarshal(data, &params.Params)
	if err != nil {
		logger.Errorf("JobParameters のデシリアライズに失敗しました: %v", err)
		return exception.NewBatchError(module, "JobParameters のデシリアライズに失敗しました", err, false, false)
	}
	logger.Debugf("JobParameters のデシリアライズが完了しました。")
	return nil
}

// MarshalFailures は []error を JSON バイトスライスにシリアライズします。
// error インターフェースは直接JSON化できないため、エラーメッセージの文字列スライスに変換します。
func MarshalFailures(failures []error) ([]byte, error) {
	module := "serialization"
	logger.Debugf("Failures のシリアライズを開始します。")

	if failures == nil {
		logger.Debugf("Failures が nil です。空のJSON配列を返します。")
		return []byte("[]"), nil
	}
	msgs := make([]string, len(failures))
	for i, err := range failures {
		msgs[i] = err.Error()
	}
	data, err := json.Marshal(msgs)
	if err != nil {
		logger.Errorf("Failures のシリアライズに失敗しました: %v", err)
		return nil, exception.NewBatchError(module, "Failures のシリアライズに失敗しました", err, false, false)
	}
	logger.Debugf("Failures のシリアライズが完了しました。")
	return data, nil
}

// UnmarshalFailures は JSON バイトスライスを []error にデシリアライズします。
// 文字列スライスとしてデコードし、それぞれを fmt.Errorf で error 型に戻します。
func UnmarshalFailures(data []byte) ([]error, error) {
	module := "serialization"
	logger.Debugf("Failures のデシリアライズを開始します。データサイズ: %d バイト", len(data))

	if len(data) == 0 || string(data) == "null" {
		logger.Debugf("Failures が nil または空データです。空のスライスを返します。")
		return []error{}, nil
	}
	var msgs []string
	err := json.Unmarshal(data, &msgs)
	if err != nil {
		logger.Errorf("Failures のデシリアライズに失敗しました: %v", err)
		return nil, exception.NewBatchError(module, "Failures のデシリアライズに失敗しました", err, false, false)
	}
	failures := make([]error, len(msgs))
	for i, msg := range msgs {
		failures[i] = fmt.Errorf(msg)
	}
	logger.Debugf("Failures のデシリアライズが完了しました。")
	return failures, nil
}
