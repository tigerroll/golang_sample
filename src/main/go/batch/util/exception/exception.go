package exception

import "fmt"
import core "sample/src/main/go/batch/job/core" // core パッケージをインポート
import "encoding/json" // json パッケージをインポート


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
func marshalExecutionContext(ctx core.ExecutionContext) ([]byte, error) {
  if ctx == nil {
    // nil の場合は空の JSON オブジェクトを表すバイトスライスを返す
    return []byte("{}"), nil
  }
  // JSON にシリアライズ
  data, err := json.Marshal(ctx)
  if err != nil {
    // シリアライズ失敗時はエラーを返す
    return nil, fmt.Errorf("ExecutionContext のシリアライズに失敗しました: %w", err)
  }
  return data, nil
}

// unmarshalExecutionContext は JSON バイトスライスを ExecutionContext にデシリアライズします。
// デシリアライズ結果は既存の ExecutionContext マップに格納されます。
// 既存のマップが nil の場合は新しいマップを作成します。
func unmarshalExecutionContext(data []byte, ctx *core.ExecutionContext) error {
  if len(data) == 0 || string(data) == "null" {
    // データが空または "null" の場合は、ExecutionContext を空にする
    if *ctx == nil {
      *ctx = core.NewExecutionContext() // nil の場合は新しいマップを作成
    } else {
      // 既存のマップをクリア
      for k := range *ctx {
        delete(*ctx, k)
      }
    }
    return nil
  }

  // デシリアライズ対象のマップを初期化またはクリア
  if *ctx == nil {
    *ctx = core.NewExecutionContext() // nil の場合は新しいマップを作成
  } else {
    // 既存のマップをクリア
    for k := range *ctx {
      delete(*ctx, k)
    }
  }

  // JSON からマップにデシリアライズ
  err := json.Unmarshal(data, ctx)
  if err != nil {
    // デシリアライズ失敗時はエラーを返す
    return fmt.Errorf("ExecutionContext のデシリアライズに失敗しました: %w", err)
  }
  return nil
}


// TODO: Failureliye の永続化・復元ヘルパー関数またはメソッドを追加 (必要に応じて検討)
// error 型は Marshal/Unmarshal が標準でサポートしていないため、エラーメッセージの文字列リストとして保存するなどの工夫が必要
// func marshalErrors(errs []error) ([]byte, error) { ... }
// func unmarshalErrors(data []byte) ([]error, error) { ... }