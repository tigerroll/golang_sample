package exception

import "fmt"
// import core "sample/src/main/go/batch/job/core" // core パッケージをインポート - 削除
// import "encoding/json" // json パッケージをインポート - 削除


// BatchError はカスタムエラー型です。
type BatchError struct {
	Message string
	Module  string // エラーが発生したモジュール (例: reader, processor, writer, config)
	Err     error  // 元のエラー (wrap)
}

// Error は error インターフェースを実装します。
func (e *BatchError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Module, e.Message, e.Err)
	}
	return fmt.Sprintf("[%s] %s", e.Module, e.Message)
}

// Unwrap は errors.Unwrap をサポートします。
func (e *BatchError) Unwrap() error {
	return e.Err
}

// NewBatchError は新しい BatchError を作成します。
func NewBatchError(module, message string, err error) *BatchError {
	return &BatchError{
		Message: message,
		Module:  module,
		Err:     err,
	}
}

// NewBatchErrorf はフォーマットされたメッセージで新しい BatchError を作成します。
func NewBatchErrorf(module, format string, v ...interface{}) *BatchError {
	message := fmt.Sprintf(format, v...)
	return &BatchError{
		Message: message,
		Module:  module,
	}
}

// IsBatchError はエラーが BatchError 型かどうかを判定します。
func IsBatchError(err error) bool {
	_, ok := err.(*BatchError)
	return ok
}

// marshalExecutionContext は ExecutionContext を JSON バイトスライスにシリアライズします。
// この関数は src/main/go/batch/util/serialization/serialization.go に移動されました。
// func marshalExecutionContext(ctx core.ExecutionContext) ([]byte, error) { ... }


// unmarshalExecutionContext は JSON バイトスライスを ExecutionContext にデシリアライズします。
// この関数は src/main/go/batch/util/serialization/serialization.go に移動されました。
// func unmarshalExecutionContext(data []byte, ctx *core.ExecutionContext) error { ... }


// TODO: Failureliye の永続化・復元ヘルパー関数またはメソッドを追加 (必要に応じて検討)
// error 型は Marshal/Unmarshal が標準でサポートしていないため、エラーメッセージの文字列リストとして保存するなどの工夫が必要
// func marshalErrors(errs []error) ([]byte, error) { ... }
// func unmarshalErrors(data []byte) ([]error, error) { ... }
