package myapp // テストコードも同じパッケージ名にする

import "testing"

func TestMessage(t *testing.T) {
    expected := "hello, world"
    actual := Message()

    if actual != expected {
        t.Errorf("Message() returned '%s', expected '%s'", actual, expected)
    }
}
