---
artifact_type: discovery_report
source_name: "{{ source_name }}"
generated_at: "{{ generated_at }}"
status: "{{ complete_or_partial }}"
---

# Discovery Report — {{ source_name }}

## Scope And Method

- Requested boundary: {{ boundary }}
- Evidence file: `observations.csv`
- Methods and filters: {{ methods_and_filters }}
- Exclusions: {{ exclusions_or_none }}

## Source Summary

{{ concise_source_and_access_summary }}

## Inventory Summary

{{ object_counts_key_relationships_and_material_volume_signals }}

## Watermark Assessment

{{ generated_watermark_assessment_table }}

Generate this table with `finalize_watermark_assessment.py`; do not hand-maintain a second copy of
object decisions. A shortlist is not confirmation.

## Operational Constraints

{{ access_windows_rate_limits_network_or_source_constraints }}

## Limitations And Failed Probes

{{ limitations_failures_and_unretained_sensitive_evidence_or_none }}

## Handoff

{{ evidence_paths_and_decisions_the_next_skill_still_needs_to_make }}

## Unresolved Questions

{{ unresolved_questions_or_none }}
