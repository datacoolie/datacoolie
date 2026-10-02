"""Generate the public runtime-configuration reference from source models."""

import mkdocs_gen_files

from _runtime_reference import render_runtime_reference


with mkdocs_gen_files.open("reference/runtime-configuration.md", "w") as fp:
    fp.write(render_runtime_reference())
