package exception

import "fmt"

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
