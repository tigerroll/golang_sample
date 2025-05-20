package serialization

import (
  "encoding/json"
  "fmt"

  core "sample/src/main/go/batch/job/core" // core パッケージをインポート
)

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
