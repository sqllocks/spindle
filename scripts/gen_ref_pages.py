"""Generate API reference pages for mkdocstrings.

This script is called by the mkdocs-gen-files plugin during build.
It walks the sqllocks_spindle package and creates a reference page
for each module with a ::: directive for mkdocstrings.
"""

from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()
root = Path("sqllocks_spindle")

SKIP_PATTERNS = {
    "__pycache__",
    "MacBook Pro",
    ".pyc",
}

for path in sorted(root.rglob("*.py")):
    # Skip unwanted files
    if any(pat in str(path) for pat in SKIP_PATTERNS):
        continue

    module_path = path.relative_to(root.parent).with_suffix("")
    doc_path = path.relative_to(root.parent).with_suffix(".md")
    full_doc_path = Path("reference", doc_path)

    parts = tuple(module_path.parts)

    # Skip __init__ from nav but still generate the page
    if parts[-1] == "__init__":
        parts = parts[:-1]
        if not parts:
            continue
        doc_path = doc_path.with_name("index.md")
        full_doc_path = full_doc_path.with_name("index.md")

    # Build the nav entry
    nav[parts] = doc_path.as_posix()

    # Write the reference page
    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        identifier = ".".join(parts)
        fd.write(f"::: {identifier}\n")

    mkdocs_gen_files.set_edit_path(full_doc_path, path.as_posix())

# Write the SUMMARY.md for literate-nav
with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
