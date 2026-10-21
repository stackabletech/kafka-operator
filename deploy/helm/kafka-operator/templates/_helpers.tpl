{{/*
Expand the name of the chart.
*/}}
{{- define "kafka-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-operator" }}
{{- end }}

{{/*
Expand the name of the chart.
*/}}
{{- define "kafka-operator.appname" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "kafka-operator.fullname" -}}
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
{{- define "kafka-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "kafka-operator.labels" -}}
helm.sh/chart: {{ include "kafka-operator.chart" . }}
{{ include "kafka-operator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "kafka-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kafka-operator.appname" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- with .Values.labels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "kafka-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (printf "%s-serviceaccount" (include "kafka-operator.fullname" .)) .Values.serviceAccount.name }}
{{- else }}
{{- required "serviceAccount.name is required when serviceAccount.create is false, because the chart then does not create a ServiceAccount for the operator to run as." .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Labels for Kubernetes objects created by helm test
*/}}
{{- define "kafka-operator.testLabels" -}}
helm.sh/test: {{ include "kafka-operator.chart" . }}
{{- end }}

{{/*
Build the full operator container image reference.
*/}}
{{- define "kafka-operator.image" -}}
{{- printf "%s/%s:%s" .Values.image.repository .Chart.Name (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end }}
