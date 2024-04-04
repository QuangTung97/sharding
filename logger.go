package sharding

import (
	"log"
)

type defaultLoggerImpl struct {
}

func (*defaultLoggerImpl) Infof(format string, args ...any) {
	log.Printf("[INFO] [SHARDING] "+format, args...)
}

func (*defaultLoggerImpl) Warnf(format string, args ...any) {
	log.Printf("[WARN] [SHARDING] "+format, args...)
}

func (*defaultLoggerImpl) Errorf(format string, args ...any) {
	log.Printf("[ERROR] [SHARDING] "+format, args...)
}
