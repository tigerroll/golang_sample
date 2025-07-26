// pkg/batch/step/writer/execution_context_writer.go
package writer

import (
	"context"
	"database/sql" // トランザクションを受け取るため

	core "github.com/tigerroll/go_sample/pkg/batch/job/core"
	logger "github.com/tigerroll/go_sample/pkg/batch/util/