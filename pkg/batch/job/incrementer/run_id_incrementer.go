package incrementer

import (
	"fmt"

	core "sample/pkg/batch/job/core"
	logger "sample/pkg/batch/util/logger"
)

// RunIDIncrementer はジョブパラメータに "run.id" を追加またはインクリメントする JobParametersIncrementer の実装です。
// "run.id" が存在しない場合は 1 を設定し、存在する場合はその値をインクリメントします。
type RunIDIncrementer struct {
	name string
}

// NewRunIDIncrementer は新しい RunIDIncrementer のインスタンスを作成します。
func NewRunIDIncrementer(name string) *RunIDIncrementer {
	return &RunIDIncrementer{
		name: name,
	}
}

// GetNext は与えられた JobParameters に "run.id" を追加またはインクリメントして返します。
func (i *RunIDIncrementer) GetNext(params core.JobParameters) core.JobParameters {
	nextParams := core.NewJobParameters()
	// 既存のパラメータを全てコピー
	for k, v := range params.Params {
		nextParams.Put(k, v)
	}

	currentRunID, ok := params.GetInt(i.name)
	if !ok {
		// "run.id" が存在しない場合は 1 を設定
		nextParams.Put(i.name, 1)
		logger.Debugf("JobParametersIncrementer '%s': '%s' が見つからないため、1 を設定しました。", i.name, i.name)
	} else {
		// 存在する場合はインクリメント
		nextRunID := currentRunID + 1
		nextParams.Put(i.name, nextRunID)
		logger.Debugf("JobParametersIncrementer '%s': '%s' を %d から %d にインクリメントしました。", i.name, i.name, currentRunID, nextRunID)
	}

	return nextParams
}

// String は RunIDIncrementer の文字列表現を返します。
func (i *RunIDIncrementer) String() string {
	return fmt.Sprintf("RunIDIncrementer[name=%s]", i.name)
}

// Ensure RunIDIncrementer implements core.JobParametersIncrementer
var _ core.JobParametersIncrementer = (*RunIDIncrementer)(nil)
