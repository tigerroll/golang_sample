package exception

import (
	"fmt"
	"runtime" // runtime パッケージをインポート
	"strings" // strings パッケージをインポート
)

// BatchError はバッチ処理中に発生するカスタムエラー型です。
// エラーの発生元モジュール、メッセージ、ラップされた元のエラー、
// そしてリトライ可能か、スキップ可能かのフラグを保持します。
type BatchError struct {
	Module      string // エラーが発生したモジュール (例: "reader", "processor", "writer", "config")
	Message     string // エラーの簡潔な説明
	OriginalErr error  // ラップされた元のエラー
	IsRetryable bool   // このエラーがリトライ可能か
	IsSkippable bool   // このエラーがスキップ可能か
	StackTrace  string // スタックトレース (デバッグ用)
}

// NewBatchError は新しい BatchError のインスタンスを作成します。
// isRetryable と isSkippable フラグを追加
func NewBatchError(module, message string, originalErr error, isRetryable, isSkippable bool) *BatchError {
	// スタックトレースをキャプチャ (デバッグ用途)
	buf := make([]byte, 2048)
	n := runtime.Stack(buf, false)
	stackTrace := string(buf[:n])

	return &BatchError{
		Module:      module,
		Message:     message,
		OriginalErr: originalErr,
		IsRetryable: isRetryable,
		IsSkippable: isSkippable,
		StackTrace:  stackTrace,
	}
}

// NewBatchErrorf はフォーマット文字列を使用して新しい BatchError のインスタンスを作成します。
// isRetryable と isSkippable フラグを追加
func NewBatchErrorf(module, format string, a ...interface{}) *BatchError {
	// 最後の引数が error 型であれば OriginalErr として扱う
	var originalErr error
	if len(a) > 0 {
		if err, ok := a[len(a)-1].(error); ok {
			originalErr = err
			a = a[:len(a)-1] // OriginalErr をメッセージから除外
		}
	}

	message := fmt.Sprintf(format, a...)

	// スタックトレースをキャプチャ (デバッグ用途)
	buf := make([]byte, 2048)
	n := runtime.Stack(buf, false)
	stackTrace := string(buf[:n])

	return &BatchError{
		Module:      module,
		Message:     message,
		OriginalErr: originalErr,
		IsRetryable: false, // デフォルトは false
		IsSkippable: false, // デフォルトは false
		StackTrace:  stackTrace,
	}
}

// Error は error インターフェースの実装です。
func (e *BatchError) Error() string {
	if e.OriginalErr != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Module, e.Message, e.OriginalErr)
	}
	return fmt.Sprintf("[%s] %s", e.Module, e.Message)
}

// Unwrap は errors.Unwrap のために元のエラーを返します。
func (e *BatchError) Unwrap() error {
	return e.OriginalErr
}

// IsRetryable はこのエラーがリトライ可能かどうかを返します。
func (e *BatchError) IsRetryable() bool {
	return e.IsRetryable
}

// IsSkippable はこのエラーがスキップ可能かどうかを返します。
func (e *BatchError) IsSkippable() bool {
	return e.IsSkippable
}

// IsTemporary は一時的なエラーかどうかを判定します。
// 例えば、ネットワークエラーや一時的なDB接続エラーなど。
// これはリトライロジックで利用できます。
func IsTemporary(err error) bool {
	if err == nil {
		return false
	}
	// BatchError の IsRetryable フラグを優先
	if be, ok := err.(*BatchError); ok {
		return be.IsRetryable()
	}
	// ここに一般的な一時的エラーの判定ロジックを追加
	// 例: "timeout", "connection refused", "EOF" など
	errStr := err.Error()
	return strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "EOF") // io.EOF は通常、データの終端を示すが、ネットワークI/Oでは一時的な切断を示す場合もある
}

// IsFatal は致命的なエラーかどうかを判定します。
// これはスキップロジックで利用できます。
func IsFatal(err error) bool {
	if err == nil {
		return false
	}
	// BatchError の IsSkippable フラグを優先
	if be, ok := err.(*BatchError); ok {
		return !be.IsSkippable() // スキップ可能でないなら致命的
	}
	// ここに一般的な致命的エラーの判定ロジックを追加
	// 例: "invalid argument", "permission denied", "data corruption" など
	errStr := err.Error()
	return strings.Contains(errStr, "invalid argument") ||
		strings.Contains(errStr, "permission denied") ||
		strings.Contains(errStr, "data corruption")
}
