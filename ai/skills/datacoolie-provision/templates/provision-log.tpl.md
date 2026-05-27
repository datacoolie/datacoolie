# Provision Log — {{ project_name }}

**Date:** {{ date }}
**Architecture:** {{ architecture_path }}
**Platform:** {{ platform }}
**Mode:** {{ mode }}
**Environment:** {{ env }}
**Status:** {{ status }}

---

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
