package exception

import (
	"errors"  // errors パッケージをインポート
	"fmt"
	"reflect" // reflect パッケージをインポート
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
	isRetryable bool   // このエラーがリトライ可能か
	isSkippable bool   // このエラーがスキップ可能か
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
		isRetryable: isRetryable, // This refers to the field
		isSkippable: isSkippable, // This refers to the field
		StackTrace:  stackTrace,
	}
}

// NewBatchErrorf はフォーマット文字列を使用して新しい BatchError のインスタンスを作成します。
// isRetryable と isSkippable フラグを追加できるように変更します。
// 最後の引数が bool 型であれば isRetryable, その前の引数が bool 型であれば isSkippable として扱います。
// 例: NewBatchErrorf("module", "message %s", "arg", true, false) -> isRetryable=true, isSkippable=false
// 例: NewBatchErrorf("module", "message %s", "arg", true) -> isRetryable=true, isSkippable=false (isSkippableはデフォルト値)
// 例: NewBatchErrorf("module", "message %s", "arg", false, true, someError) -> isRetryable=false, isSkippable=true, originalErr=someError
func NewBatchErrorf(module, format string, a ...interface{}) *BatchError {
	var originalErr error
	isRetryable := false
	isSkippable := false
	args := make([]interface{}, 0, len(a))

	// 後ろから引数をチェックし、bool値とerror値を抽出
	for i := len(a) - 1; i >= 0; i-- {
		if err, ok := a[i].(error); ok && originalErr == nil {
			originalErr = err
		} else if b, ok := a[i].(bool); ok {
			if i == len(a)-1 { // 最後の引数がboolならisRetryable
				isRetryable = b
			} else if i == len(a)-2 && (reflect.TypeOf(a[len(a)-1]).Kind() != reflect.Bool) { // 最後から2番目の引数がboolで、最後の引数がboolでないならisSkippable
				isSkippable = b
			} else if i == len(a)-2 && (reflect.TypeOf(a[len(a)-1]).Kind() == reflect.Bool) { // 最後から2番目の引数がboolで、最後の引数もboolならisSkippable
				isSkippable = b
			} else {
				args = append([]interface{}{a[i]}, args...) // それ以外のboolはメッセージの一部
			}
		} else {
			args = append([]interface{}{a[i]}, args...)
		}
	}

	message := fmt.Sprintf(format, args...)

	// スタックトレースをキャプチャ (デバッグ用途)
	buf := make([]byte, 2048)
	n := runtime.Stack(buf, false)
	stackTrace := string(buf[:n])

	return &BatchError{
		Module:      module,
		Message:     message,
		OriginalErr: originalErr,
		isRetryable: isRetryable,
		isSkippable: isSkippable,
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
	return e.isRetryable
}

// IsSkippable はこのエラーがスキップ可能かどうかを返します。
func (e *BatchError) IsSkippable() bool {
	return e.isSkippable
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

// IsErrorOfType はエラーが指定された型名（文字列）に一致するかどうかを判定します。
// err は元のエラー、errorTypeName は比較するエラーの型名（例: "*net.OpError", "io.EOF"）です。
// または、エラーメッセージの一部を含む文字列（例: "connection refused"）でも判定できます。
func IsErrorOfType(err error, errorTypeName string) bool {
	if err == nil {
		return false
	}

	// 1. エラーの型名で比較
	// reflect.TypeOf(err).String() はポインタ型の場合 "*pkg.MyError" のようになる
	// reflect.TypeOf(err).Elem().String() は非ポインタ型の場合 "pkg.MyError" のようになる
	// errors.As や errors.Is を使うのがよりGoらしいが、文字列比較も柔軟性がある
	errType := reflect.TypeOf(err)
	if errType != nil && (errType.String() == errorTypeName || (errType.Kind() == reflect.Ptr && errType.Elem().String() == errorTypeName)) {
		return true
	}

	// 2. エラーメッセージの部分文字列で比較
	// これは汎用的だが、誤検知の可能性もある
	if strings.Contains(err.Error(), errorTypeName) {
		return true
	}

	// 3. ラップされたエラーを再帰的にチェック
	if unwrappedErr := errors.Unwrap(err); unwrappedErr != nil { // errors.Unwrap を使用
		return IsErrorOfType(unwrappedErr, errorTypeName)
	}

	return false
}
