package helpers

import (
	"bytes"
	"text/template"
)

func MakeTemplateFile(templateStr string, variables map[string]interface{}) (string, error) {
	t := template.Must(template.New("my_template").Parse(templateStr))
	buf := &bytes.Buffer{}
	err := t.Execute(buf, variables)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}
