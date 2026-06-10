---
artifact_type: promotion_log
project_name: "{{ project_name }}"
workspace_name: "{{ workspace_name }}"
date: "{{ date }}"
from_environment: "{{ from_env }}"
to_environment: "{{ to_env }}"
platform: "{{ platform }}"
status: "{{ status }}"
approved_gate_snapshot: '{{ approved_gate_snapshot | default("false") }}'
---

# Promotion Log - {{ from_env }} to {{ to_env }}

## Summary

- From: {{ from_env }}
- To: {{ to_env }}
- Platform: {{ platform }}
- Status: {{ status }}

## Gate Snapshot

| Gate | Status | Journal |
|---|---|---|
| {{ gate_rows }} |

## Promotion Evidence

| Check | Result | Link |
|---|---|---|
| {{ evidence_rows }} |

## Deployment Artifacts

| Artifact | Path | Status |
|---|---|---|
| {{ artifact_rows }} |

## Decision

{{ decision | default("Pending.") }}

## Rollback

{{ rollback_notes | default("TBD") }}

## Unresolved Questions

{{ unresolved_questions | default("None") }}
