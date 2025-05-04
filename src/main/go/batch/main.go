package main

import (
    "context"
    "sample/src/main/go/batch/config"
    "sample/src/main/go/batch/job"
    "sample/src/main/go/batch/util/logger"
)

func main() {
    cfg, err := config.LoadConfig()
    if err != nil {
        logger.Fatalf("設定のロードに失敗しました: %v", err)
    }

    logger.SetLogLevel(cfg.System.Logging.Level)
    logger.Infof("ログレベルを '%s' に設定しました。", cfg.System.Logging.Level)

    logger.Debugf("Database Tyoe: %s\n", cfg.Database.Type)
    logger.Debugf("Database Host: %s", cfg.Database.Host)
    logger.Debugf("Database Port: %d", cfg.Database.Port)
    logger.Debugf("Batch Polling Interval: %d", cfg.Batch.PollingIntervalSeconds)
    logger.Debugf("Batch API Endpoint: %s", cfg.Batch.APIEndpoint)

    ctx := context.Background()

    weatherJob, err := job.NewWeatherJob(ctx, cfg)
    if err != nil {
        logger.Fatalf("Weather Job の初期化に失敗しました: %v", err)
    }

    if err := weatherJob.Run(ctx); err != nil {
        logger.Fatalf("Weather Job の実行に失敗しました: %v", err)
    }

    logger.Infof("アプリケーションを終了します。")
}
