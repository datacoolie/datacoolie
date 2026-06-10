---
artifact_type: provision_log
project_name: "{{ project_name }}"
date: "{{ date }}"
architecture_path: "{{ architecture_path }}"
platform: "{{ platform }}"
mode: "{{ mode }}"
environment: "{{ env }}"
status: "{{ status }}"
---

# Provision Log — {{ project_name }}

## Resources

| # | Resource Type | Name | Action | Status | Details |
|---|---|---|---|---|---|
| {{ resource_rows }} |

---

## Commands Executed

```bash
{{ commands }}
```

---

## Summary

- **Total resources:** {{ total_count }}
- **Created:** {{ created_count }}
- **Already existed (skipped):** {{ skipped_count }}
- **Failed:** {{ failed_count }}

---

## Errors

{{ errors | default("None") }}

---

## Next Steps

{{ next_steps | default("Provisioning complete. Proceed with metadata authoring and pipeline development.") }}
