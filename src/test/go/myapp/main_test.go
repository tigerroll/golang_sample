package main

import "testing"
import mainapp "hello/src/main/go/myapp" // 別名を付けてインポート

func TestMessage(t *testing.T) {
	expected := "hello, world"
	actual := mainapp.Message() // 別名を使って関数を呼び出す

	if actual != expected {
		t.Errorf("Message() returned '%s', expected '%s'", actual, expected)
	}
}
