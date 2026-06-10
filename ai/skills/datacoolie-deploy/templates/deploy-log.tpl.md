---
artifact_type: deploy_log
project_name: "{{ project_name }}"
workspace_name: "{{ workspace_name }}"
date: "{{ date }}"
environment: "{{ env }}"
platform: "{{ platform }}"
status: "{{ status }}"
runner_artifact: "{{ runner_artifact }}"
metadata_artifact: "{{ metadata_artifact }}"
---

# Deploy Log - {{ env }}

## Summary

- Platform: {{ platform }}
- Environment: {{ env }}
- Status: {{ status }}
- Runner artifact: {{ runner_artifact }}
- Metadata artifact: {{ metadata_artifact }}

## Preflight

| Check | Result | Notes |
|---|---|---|
| {{ preflight_rows }} |

## Artifacts

| Artifact | Path | Status |
|---|---|---|
| {{ artifact_rows }} |

## Commands Executed

```bash
{{ commands }}
```

## Result

{{ result_summary }}

## Rollback

{{ rollback_notes | default("TBD") }}

## Unresolved Questions

{{ unresolved_questions | default("None") }}
