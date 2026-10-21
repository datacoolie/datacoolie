"""Publish the manifest-derived dependency inventory."""

import mkdocs_gen_files

from _dependency_reference import render_dependency_reference


with mkdocs_gen_files.open("reference/dependencies.md", "w") as fp:
    fp.write(render_dependency_reference())
