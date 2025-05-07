package job

// JobParameters はジョブ実行時のパラメータを保持する構造体です。
// Spring Batchのようにキー-値ペアのマップ形式にすることも可能ですが、
// シンプルな例として空の構造体とします。
// 必要に応じてここにフィールドを追加してください。
type JobParameters struct {
  // 例: StartDate string
  // 例: EndDate string
}

// NewJobParameters は新しい JobParameters のインスタンスを作成します。
func NewJobParameters() JobParameters {
  return JobParameters{}
}
