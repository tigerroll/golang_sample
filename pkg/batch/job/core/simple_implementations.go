package core

import (
	"context"
	"fmt"

	logger "sample/pkg/batch/util/logger"
)

// ConditionalDecision は Decision インターフェースのより柔軟な実装です。
// JSL から渡されるプロパティに基づいて動的に ExitStatus を決定します。
type ConditionalDecision struct {
	id          string
	properties  map[string]string
	conditionKey string // ExecutionContext から取得するキー
	expectedValue string // conditionKey の値がこれと一致すれば COMPLETED
	defaultStatus ExitStatus // 一致しない場合やキーが見つからない場合のデフォルトステータス
}

// NewConditionalDecision は新しい ConditionalDecision のインスタンスを作成します。
func NewConditionalDecision(id string) *ConditionalDecision {
	return &ConditionalDecision{
		id:          id,
		properties:  make(map[string]string),
		defaultStatus: ExitStatusFailed, // デフォルトは失敗
	}
}

// SetProperties は JSL から注入されるプロパティを設定します。
func (d *ConditionalDecision) SetProperties(properties map[string]string) {
	d.properties = properties
	if key, ok := properties["conditionKey"]; ok {
		d.conditionKey = key
	}
	if val, ok := properties["expectedValue"]; ok {
		d.expectedValue = val
	}
	if statusStr, ok := properties["defaultStatus"]; ok {
		d.defaultStatus = ExitStatus(statusStr)
	}
	logger.Debugf("ConditionalDecision '%s': プロパティを設定しました。conditionKey='%s', expectedValue='%s', defaultStatus='%s'",
		d.id, d.conditionKey, d.expectedValue, d.defaultStatus)
}

// Decide は ExecutionContext の値に基づいて ExitStatus を決定します。
func (d *ConditionalDecision) Decide(ctx context.Context, jobExecution *JobExecution, jobParameters JobParameters) (ExitStatus, error) {
	logger.Debugf("ConditionalDecision '%s' が呼び出されました。conditionKey='%s', expectedValue='%s'", d.id, d.conditionKey, d.expectedValue)

	if d.conditionKey == "" {
		logger.Warnf("ConditionalDecision '%s': conditionKey が設定されていません。デフォルトステータス '%s' を返します。", d.id, d.defaultStatus)
		return d.defaultStatus, nil
	}

	// まず JobExecutionContext を確認
	actualValue, ok := jobExecution.ExecutionContext.GetNested(d.conditionKey)
	if !ok {
		// 次に StepExecutionContext を確認 (現在のステップの ExecutionContext)
		// ただし、Decision はステップではないため、通常は JobExecutionContext を参照する
		// ここではシンプルに JobExecutionContext のみを見る
		logger.Warnf("ConditionalDecision '%s': JobExecutionContext にキー '%s' が見つかりません。デフォルトステータス '%s' を返します。", d.id, d.conditionKey, d.defaultStatus)
		return d.defaultStatus, nil
	}

	// 取得した値を文字列に変換して比較
	actualValueStr := fmt.Sprintf("%v", actualValue)
	if actualValueStr == d.expectedValue {
		logger.Infof("ConditionalDecision '%s': 条件が一致しました ('%s' == '%s')。ExitStatusCompleted を返します。", d.id, actualValueStr, d.expectedValue)
		return ExitStatusCompleted, nil
	}

	logger.Infof("ConditionalDecision '%s': 条件が一致しませんでした ('%s' != '%s')。デフォルトステータス '%s' を返します。", d.id, actualValueStr, d.expectedValue, d.defaultStatus)
	return d.defaultStatus, nil
}

// DecisionName は Decision の名前を返します。
func (d *ConditionalDecision) DecisionName() string {
	return d.id
}

// ID は Decision のIDを返します。
func (d *ConditionalDecision) ID() string {
	return d.id
}
