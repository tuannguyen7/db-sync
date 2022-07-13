package helpers

import (
	"time"

	"github.com/sirupsen/logrus"
)

func Elapsed(l *logrus.Entry) func() {
	start := time.Now()
	return func() {
		l.Infoln("function took %v", time.Since(start))
	}
}
