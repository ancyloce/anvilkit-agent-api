{{/* The stable service identity (architecture.md naming): Deployment, Service,
ServiceAccount and Helm release share it. */}}
{{- define "anvilkit-agent-api.name" -}}
anvilkit-agent-api
{{- end -}}

{{- define "anvilkit-agent-api.fullname" -}}
{{- if eq .Release.Name (include "anvilkit-agent-api.name" .) -}}
{{- .Release.Name -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "anvilkit-agent-api.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "anvilkit-agent-api.labels" -}}
app.kubernetes.io/name: {{ include "anvilkit-agent-api.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/component: api
app.kubernetes.io/part-of: anvilkit
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{- define "anvilkit-agent-api.selectorLabels" -}}
app.kubernetes.io/name: {{ include "anvilkit-agent-api.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "anvilkit-agent-api.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "anvilkit-agent-api.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* The image reference: a digest pins the exact image, otherwise the tag
(defaulting to the chart's appVersion). */}}
{{- define "anvilkit-agent-api.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{/* Required environment values, checked once for every template. */}}
{{- define "anvilkit-agent-api.require" -}}
{{- if not .Values.control.address }}
{{- fail "control.address is required: the Control gRPC endpoint (ANVILKIT_API_CONTROL_ADDRESS)" }}
{{- end }}
{{- if and (eq .Values.auth.mode "fixture") (not .Values.auth.principalsSecret.name) }}
{{- fail "auth.principalsSecret.name is required in fixture mode: an existing Secret holding the principals file (never created by this chart)" }}
{{- end }}
{{- if not (has .Values.identity.mode (list "mtls" "development")) }}
{{- fail "identity.mode must be mtls or development" }}
{{- end }}
{{- if and (eq .Values.identity.mode "development") (not .Values.development.enabled) }}
{{- fail "identity.mode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true (plaintext clients present no workload identity)" }}
{{- end }}
{{- if and (eq .Values.identity.mode "mtls") (not .Values.identity.trustDomain) (not .Values.development.enabled) }}
{{- fail "identity.trustDomain is required outside development (the development default anvilkit.local applies only with development.enabled: true)" }}
{{- end }}
{{- if and (eq .Values.identity.mode "mtls") .Values.identity.certificate.create (not .Values.identity.certificate.issuerRef.name) }}
{{- fail "identity.certificate.issuerRef.name is required: the cert-manager issuer of the workload certificate (or set identity.certificate.create false and identity.secretName)" }}
{{- end }}
{{- if and (eq .Values.identity.mode "mtls") (not .Values.identity.certificate.create) (not .Values.identity.secretName) }}
{{- fail "identity.secretName is required while identity.certificate.create is false" }}
{{- end }}
{{- if .Values.telemetry.otlpEndpoint }}
{{- if not (has .Values.telemetry.otlpTls.mode (list "tls" "mtls" "development")) }}
{{- fail "telemetry.otlpTls.mode must be tls, mtls or development" }}
{{- end }}
{{- if and (eq .Values.telemetry.otlpTls.mode "development") (not .Values.development.enabled) }}
{{- fail "telemetry.otlpTls.mode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true" }}
{{- end }}
{{- if and (ne .Values.telemetry.otlpTls.mode "development") (not .Values.telemetry.otlpTls.caSecret.name) }}
{{- fail "telemetry.otlpTls.caSecret.name is required under telemetry.otlpTls.mode tls or mtls" }}
{{- end }}
{{- end }}
{{- end -}}

{{/* The identity Secret: the rendered Certificate's or the environment's. */}}
{{- define "anvilkit-agent-api.identitySecret" -}}
{{- if .Values.identity.certificate.create -}}
{{- printf "%s-identity" (include "anvilkit-agent-api.fullname" .) -}}
{{- else -}}
{{- .Values.identity.secretName -}}
{{- end -}}
{{- end -}}

{{/* The rendered configuration: the reviewed sections plus the chart-owned
identity, guard and transport settings, so every value above is wired to
the loader's keys. */}}
{{- define "anvilkit-agent-api.config" -}}
{{- $cfg := deepCopy .Values.config -}}
{{- $_ := set $cfg "development" (dict "enabled" .Values.development.enabled) -}}
{{- if eq .Values.identity.mode "mtls" }}
{{- $_ = set $cfg "identity" (dict "cert_file" "/etc/anvilkit/identity/tls.crt" "key_file" "/etc/anvilkit/identity/tls.key" "ca_file" "/etc/anvilkit/identity/ca.crt") }}
{{- end -}}
{{- $_ = set $cfg.control "identity" (dict "mode" .Values.identity.mode "mtls" (dict "server_name" .Values.control.serverName)) -}}
{{- $_ = set $cfg "knowledge" (merge (dict "identity" (dict "mode" .Values.identity.mode "mtls" (dict "server_name" .Values.knowledge.serverName))) (default (dict) $cfg.knowledge)) -}}
{{- $_ = set $cfg "mcp" (merge (dict "identity" (dict "mode" .Values.identity.mode "mtls" (dict "server_name" .Values.mcp.serverName))) (default (dict) $cfg.mcp)) -}}
{{- if .Values.telemetry.otlpEndpoint }}
{{- $ot := dict "mode" .Values.telemetry.otlpTls.mode -}}
{{- if ne .Values.telemetry.otlpTls.mode "development" }}
{{- $_ = set $ot "ca_file" "/etc/anvilkit/otlp-ca/ca.crt" }}
{{- if .Values.telemetry.otlpTls.serverName }}{{- $_ = set $ot "server_name" .Values.telemetry.otlpTls.serverName }}{{- end }}
{{- end -}}
{{- $_ = set $cfg "telemetry" (merge (dict "otlp_tls" $ot) (default (dict) $cfg.telemetry)) -}}
{{- end -}}
{{- toYaml $cfg -}}
{{- end -}}
