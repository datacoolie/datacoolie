# Hash-bound design approval commands

Use the selected Python environment. `$design` is the absolute installed `datacoolie-design` skill
directory and `$workspace` is the absolute project workspace. Finalize
`$workspace/architecture/current.md` before hashing:

```powershell
& $python "$design/scripts/design_approval.py" hash --workspace $workspace --architecture "$workspace/architecture/current.md"
```

Present the exact candidate, hash, material choices and unresolved questions for explicit
current-session approval. Only after that approval, set `$approvedBy`, `$approvalReference` and
`$approvedScope` to the actual evidence and run:

```powershell
& $python "$design/scripts/design_approval.py" record --workspace $workspace --architecture "$workspace/architecture/current.md" --approved-by $approvedBy --approval-reference $approvalReference --approved-scope $approvedScope --confirmed
& $python "$design/scripts/design_approval.py" verify --workspace $workspace --architecture "$workspace/architecture/current.md"
```

Record prints the receipt path; verify prints the current full SHA-256 on success. A nonzero exit
blocks dependent Build work. Any architecture byte change requires renewed approval and a matching
receipt. These commands persist approval evidence; they do not create approval or authorize
Provision/Release.
