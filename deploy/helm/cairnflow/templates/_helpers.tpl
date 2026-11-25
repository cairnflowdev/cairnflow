{{- define "cairnflow.name" -}}{{ .Release.Name }}-cairnflow{{- end -}}
{{- define "cairnflow.labels" -}}
app.kubernetes.io/name: cairnflow
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
