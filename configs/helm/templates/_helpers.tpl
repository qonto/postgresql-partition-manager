{{/*
Expand the name of the chart.
*/}}
{{- define "postgresql-partition-manager.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "postgresql-partition-manager.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "postgresql-partition-manager.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "postgresql-partition-manager.labels" -}}
helm.sh/chart: {{ include "postgresql-partition-manager.chart" . }}
{{ include "postgresql-partition-manager.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/component: cronjob
app.kubernetes.io/part-of: postgresql-partition-manager
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- range $key, $value := .Values.additionalLabels }}
{{ $key }}: {{ $value | quote }}
{{- end -}}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "postgresql-partition-manager.selectorLabels" -}}
app.kubernetes.io/name: {{ include "postgresql-partition-manager.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
