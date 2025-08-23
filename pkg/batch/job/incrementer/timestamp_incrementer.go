package incrementer

import (
	"fmt"
	"strconv"
	"time"

	core "sample/pkg/batch/job/core"
	logger "sample/pkg/batch/util/logger"
)

// TimestampIncrementer はジョブパラメータに "timestamp" を追加する JobParametersIncrementer の実装です。
// "timestamp" が存在しない場合は現在時刻のUnixミリ秒を設定し、存在する場合はその値を更新します。
type TimestampIncrementer struct {
	name string
}

// NewTimestampIncrementer は新しい TimestampIncrementer のインスタンスを作成します。
func NewTimestampIncrementer(name string) *TimestampIncrementer {
	return &TimestampIncrementer{
		name: name,
	}
}

// GetNext は与えられた JobParameters に "timestamp" を追加または更新して返します。
func (i *TimestampIncrementer) GetNext(params core.JobParameters) core.JobParameters {
	nextParams := core.NewJobParameters()
	// 既存のパラメータを全てコピー
	for k, v := range params.Params {
		nextParams.Put(k, v)
	}

	// 現在時刻のUnixミリ秒を設定
	timestamp := time.Now().UnixMilli()
	nextParams.Put(i.name, strconv.FormatInt(timestamp, 10)) // i.name を使用し、文字列として保存
	logger.Debugf("JobParametersIncrementer '%s': '%s' を %d に設定しました。", i.name, i.name, timestamp)

	return nextParams
}

// String は TimestampIncrementer の文字列表現を返します。
func (i *TimestampIncrementer) String() string {
	return fmt.Sprintf("TimestampIncrementer[name=%s]", i.name)
}

// Ensure TimestampIncrementer implements core.JobParametersIncrementer
var _ core.JobParametersIncrementer = (*TimestampIncrementer)(nil)
