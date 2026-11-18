{{- define "workload.supervisorName" -}}
{{- $identity := printf "%s:%s:%s" .Release.Namespace .Release.Name (.Files.Get "writer-supervisor.py") -}}
{{- printf "%s-writer-%s" ((include "workload.name" .) | trunc 43 | trimSuffix "-") ($identity | sha256sum | trunc 12) -}}
{{- end -}}

{{- define "workload.name" -}}{{ printf "%s-collector" .Release.Name | trunc 63 | trimSuffix "-" }}{{- end -}}

{{- define "workload.image" -}}
{{- $image := required "a reviewed image digest is required" . -}}
{{- if not (regexMatch "^[a-z0-9][a-z0-9./:_-]*@sha256:[a-f0-9]{64}$" $image) -}}
{{- fail "image must be a repository pinned by SHA-256 digest" -}}
{{- end -}}
{{- $image | quote -}}
{{- end -}}

{{- define "workload.validate" -}}
{{- if not (has .Values.mode (list "sidecar" "daemonSet")) -}}
{{- fail "mode must be sidecar or daemonSet" -}}
{{- end -}}
{{- if not (regexMatch "^https://[a-z0-9]([a-z0-9.-]*[a-z0-9])?(:[0-9]{1,5})?$" .Values.identity.origin) -}}
{{- fail "identity.origin must be one exact HTTPS origin without a path, query or credentials" -}}
{{- end -}}
{{- if not (has .Values.transcripts.provider (list "codex" "claude" "gemini")) -}}
{{- fail "select one qualified transcript provider: codex, claude or gemini" -}}
{{- end -}}
{{- if not (regexMatch "^[a-z0-9][a-z0-9._:/-]{0,127}$" .Values.location.provider) -}}
{{- fail "location provider must be a lowercase identifier" -}}
{{- end -}}
{{- $label := .Values.location.label -}}
{{- if or (empty $label) (ne $label (trim $label)) (gt (len $label) 128) (regexMatch "[\\p{Cc}\\p{Cf}]" $label) -}}
{{- fail "location label must be trimmed printable UTF-8, at most 128 bytes" -}}
{{- end -}}
{{- range $field := list .Values.location.cluster .Values.location.workload -}}
{{- if not (regexMatch "^[a-zA-Z0-9._:/-]{1,128}$" $field) -}}
{{- fail "location cluster and workload must be explicit bounded identifiers" -}}
{{- end -}}
{{- end -}}
{{- if or (lt (int .Values.collector.uid) 1) (gt (int64 .Values.collector.uid) 2147483647) -}}
{{- fail "collector UID must be non-root" -}}
{{- end -}}
{{- if or (lt (int .Values.collector.flushSeconds) 1) (gt (int .Values.collector.flushSeconds) 20)
           (lt (int .Values.writer.shutdownSeconds) 0) (gt (int .Values.writer.shutdownSeconds) 120)
           (lt (int .Values.terminationGracePeriodSeconds) 30)
           (gt (int .Values.terminationGracePeriodSeconds) 300) -}}
{{- fail "writer, collector and pod termination budgets must be bounded" -}}
{{- end -}}
{{- $needed := add (int .Values.collector.flushSeconds) 5 -}}
{{- if eq .Values.mode "sidecar" -}}
{{- $needed = add $needed (int .Values.writer.shutdownSeconds) -}}
{{- end -}}
{{- if lt (int64 .Values.terminationGracePeriodSeconds) $needed -}}
{{- fail "pod grace must cover writer shutdown, collector flush and five seconds of overhead" -}}
{{- end -}}
{{- if or (lt (int64 .Values.collector.spoolLimitBytes) 1) (gt (int64 .Values.collector.spoolLimitBytes) 536870912) -}}
{{- fail "collector spool must have an explicit bound up to 512 MiB" -}}
{{- end -}}
{{- end -}}

{{- define "workload.collector" -}}
name: collector
image: {{ include "workload.image" .Values.collector.image }}
imagePullPolicy: IfNotPresent
command: ["/usr/local/bin/csl-workload-collector"]
{{- if eq .Values.mode "sidecar" }}
restartPolicy: Always
{{- end }}
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  runAsNonRoot: true
  capabilities:
    drop: ["ALL"]
env:
  - name: VITRAL_COLLECTOR_CLIENT_ID
    value: vitral-workload-collector
  - name: VITRAL_COLLECTOR_ORIGIN
    value: {{ .Values.identity.origin | quote }}
  - name: VITRAL_COLLECTOR_PROVIDER
    value: {{ .Values.transcripts.provider | quote }}
  - name: VITRAL_COLLECTOR_TRANSCRIPT_ROOT
    value: /transcripts
  - name: VITRAL_COLLECTOR_STATE
    value: /collector-state
  - name: VITRAL_COLLECTOR_REDACTION
    value: "required"
  - name: VITRAL_COLLECTOR_FLUSH_SECONDS
    value: {{ .Values.collector.flushSeconds | quote }}
  - name: VITRAL_COLLECTOR_SPOOL_LIMIT_BYTES
    value: {{ .Values.collector.spoolLimitBytes | quote }}
  - name: VITRAL_LOCATION_LABEL
    value: {{ .Values.location.label | quote }}
  - name: VITRAL_LOCATION_PROVIDER
    value: {{ .Values.location.provider | quote }}
  - name: VITRAL_LOCATION_CLUSTER
    value: {{ .Values.location.cluster | quote }}
  - name: VITRAL_LOCATION_WORKLOAD
    value: {{ .Values.location.workload | quote }}
  - name: VITRAL_LOCATION_NAMESPACE
    valueFrom:
      fieldRef:
        fieldPath: metadata.namespace
  - name: VITRAL_LOCATION_REPLICA
    valueFrom:
      fieldRef:
        fieldPath: metadata.uid
{{- if .Values.identity.caConfigMap }}
  - name: VITRAL_COLLECTOR_TRUST_CA
    value: /collector-trust/ca.crt
{{- end }}
resources:
  requests:
    cpu: 50m
    memory: 64Mi
  limits:
    memory: 256Mi
volumeMounts:
  - name: transcript
    mountPath: /transcripts
    readOnly: true
  - name: collector-state
    mountPath: /collector-state
{{- if .Values.identity.caConfigMap }}
  - name: collector-trust
    mountPath: /collector-trust
    readOnly: true
{{- end }}
{{- end -}}
