package jsl

import (
	"fmt"
	"gopkg.in/yaml.v3"

	"sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"
)

// LoadedJobDefinitions holds all loaded JSL job definitions.
// このマップは、JobFactory がジョブIDで定義を取得するために必要です。
var LoadedJobDefinitions = make(map[string]Job)

// LoadJSLDefinitionFromBytes は単一のJSL YAMLファイルのバイトデータからジョブ定義をロードします。
// この関数はアプリケーション側で埋め込まれた単一のJSLファイルをロードするために使用されます。
func LoadJSLDefinitionFromBytes(data []byte) error {
	logger.Infof("JSL 定義のロードを開始します。")

	var jobDef Job
	if err := yaml.Unmarshal(data, &jobDef); err != nil {
		return exception.NewBatchError("jsl_loader", "JSL ファイルのパースに失敗しました", err, false, false)
	}

	if jobDef.ID == "" {
		return exception.NewBatchError("jsl_loader", "JSL ファイルに 'id' が定義されていません", nil, false, false)
	}
	if jobDef.Name == "" {
		return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ジョブ '%s' に 'name' が定義されていません", jobDef.ID), nil, false, false)
	}
	if jobDef.Flow.StartElement == "" {
		return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ジョブ '%s' のフローに 'start-element' が定義されていません", jobDef.ID), nil, false, false)
	}
	if len(jobDef.Flow.Elements) == 0 {
		return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ジョブ '%s' のフローに 'elements' が定義されていません", jobDef.ID), nil, false, false)
	}

	if _, exists := LoadedJobDefinitions[jobDef.ID]; exists {
		return exception.NewBatchError("jsl_loader", fmt.Sprintf("JSL ジョブID '%s' が重複しています", jobDef.ID), nil, false, false)
	}

	LoadedJobDefinitions[jobDef.ID] = jobDef
	logger.Infof("JSL ジョブ '%s' をロードしました。", jobDef.ID)
	logger.Infof("JSL 定義のロードが完了しました。ロードされたジョブ数: %d", len(LoadedJobDefinitions))
	return nil
}

// GetJobDefinition retrieves a JSL Job definition by its ID.
func GetJobDefinition(jobID string) (Job, bool) {
	job, ok := LoadedJobDefinitions[jobID]
	return job, ok
}

// GetLoadedJobCount returns the number of loaded JSL job definitions.
func GetLoadedJobCount() int {
	return len(LoadedJobDefinitions)
}
