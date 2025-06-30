{{- define "monitoring.enabled" -}}
{{- if eq (lower .Values.monitoring.disableByEnv | default "false") "true" -}}
false
{{- else -}}
true
{{- end -}}
{{- end -}}
