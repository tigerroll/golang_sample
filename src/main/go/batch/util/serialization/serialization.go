package serialization

import (
  "encoding/json"

  core "sample/src/main/go/batch/job/core" // core パッケージをインポート
  "sample/src/main/go/batch/util/exception" // exception パッケージをインポート
  logger "sample/src/main/go/batch/util/logger" // logger パッケージをインポート
)

// MarshalExecutionContext は ExecutionContext を JSON バイトスライスにシリアライズします。
// パッケージ外から参照できるように関数名をエクスポート (先頭を大文字に)
func MarshalExecutionContext(ctx core.ExecutionContext) ([]byte, error) {
  module := "serialization" // このモジュールの名前を定義
  logger.Debugf("ExecutionContext のシリアライズを開始します。")

  if ctx == nil {
    // nil の場合は空の JSON オブジェクトを表すバイトスライスを返す
    logger.Debugf("ExecutionContext が nil です。空のJSONオブジェクトを返します。")
    return []byte("{}"), nil
  }
  // JSON にシリアライズ
  data, err := json.Marshal(ctx)
  if err != nil {
    // シリアライズ失敗時はエラーを返す
    logger.Errorf("ExecutionContext のシリアライズに失敗しました: %v", err)
    return nil, exception.NewBatchError(module, "ExecutionContext のシリアライズに失敗しました", err, false, false) // BatchError でラップ
  }
  logger.Debugf("ExecutionContext のシリアライズが完了しました。")
  return data, nil
}

// UnmarshalExecutionContext は JSON バイトスライスを ExecutionContext にデシリアライズします。
// デシリアライズ結果は既存の ExecutionContext マップに格納されます。
// 既存のマップが nil の場合は新しいマップを作成します。
// パッケージ外から参照できるように関数名をエクスポート (先頭を大文字に)
func UnmarshalExecutionContext(data []byte, ctx *core.ExecutionContext) error {
  module := "serialization" // このモジュールの名前を定義
  logger.Debugf("ExecutionContext のデシリアライズを開始します。データサイズ: %d バイト", len(data))

  if len(data) == 0 || string(data) == "null" {
    // データが空または "null" の場合は、ExecutionContext を空にする
    if *ctx == nil {
      *ctx = core.NewExecutionContext() // nil の場合は新しいマップを作成
      logger.Debugf("ExecutionContext が nil または空データです。新しい空の ExecutionContext を作成しました。")
    } else {
      // 既存のマップをクリア
      for k := range *ctx {
        delete(*ctx, k)
      }
      logger.Debugf("ExecutionContext が空データです。既存の ExecutionContext をクリアしました。")
    }
    return nil
  }

  // デシリアライズ対象のマップを初期化またはクリア
  if *ctx == nil {
    *ctx = core.NewExecutionContext() // nil の場合は新しいマップを作成
    logger.Debugf("ExecutionContext が nil です。デシリアライズ用に新しいマップを作成しました。")
  } else {
    // 既存のマップをクリア
    for k := range *ctx {
      delete(*ctx, k)
    }
    logger.Debugf("既存の ExecutionContext をデシリアライズ用にクリアしました。")
  }

  // JSON からマップにデシリアライズ
  err := json.Unmarshal(data, ctx)
  if err != nil {
    // デシリアライズ失敗時はエラーを返す
    logger.Errorf("ExecutionContext のデシリアライズに失敗しました: %v", err)
    return exception.NewBatchError(module, "ExecutionContext のデシリアライズに失敗しました", err, false, false) // BatchError でラップ
  }
  logger.Debugf("ExecutionContext のデシリアライズが完了しました。")
  return nil
}
