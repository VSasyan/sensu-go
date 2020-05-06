package cmd

import (
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

var stderr = writerProxy{writer: os.Stderr}

var logger = logrus.WithFields(logrus.Fields{
	"component": "cmd",
})

type writerProxy struct {
	writer io.Writer
}

func (w writerProxy) Write(p []byte) (int, error) {
	return w.writer.Write(p)
}

func init() {
	logrus.SetOutput(stderr)
	logrus.SetFormatter(&logrus.JSONFormatter{})
}
