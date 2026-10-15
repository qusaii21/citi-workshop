"""
Build the files that JupyterHub notebook servers get for the talent insights notebook.

Usage: python3 talent_notebook_files.py <data/talent-etl dir> <output dir>

Writes into <output dir> (shipped to the notebook servers as the ConfigMap ``talent-insights``):
  talent_insights.ipynb   the analysis notebook, outputs cleared (it is run against AWS in JupyterHub)
  explore_lake.ipynb      browse the Bronze, Silver and Gold tables, outputs cleared
  package.zip.b64         the talent_etl package (top-level folders with __init__.py, except tests)
  requirements.txt        the pipeline's pinned requirements, plus matplotlib for the charts
  setup.sh                run at server start: copies the files to ~/talent-insights and installs packages
"""

import base64
import io
import json
import os
import sys
import zipfile

SETUP = r"""#!/bin/sh
# Prepares ~/talent-insights at every notebook server start (home storage is not persistent).
# Never fails the server start: problems are written to ~/talent-insights/setup.log.
DIR="$HOME/talent-insights"
mkdir -p "$DIR/notebooks"
{
  date
  python -c "import base64, io, zipfile; zipfile.ZipFile(io.BytesIO(base64.b64decode(open('/opt/talent-insights/package.zip.b64').read()))).extractall('$DIR')"
  for nb in talent_insights explore_lake; do
    [ -f "$DIR/notebooks/$nb.ipynb" ] || cp "/opt/talent-insights/$nb.ipynb" "$DIR/notebooks/"
  done
  pip install --user --no-cache-dir -q -r /opt/talent-insights/requirements.txt
  echo "setup finished"
} > "$DIR/setup.log" 2>&1 || true
"""


def main(project: str, out: str) -> None:
    os.makedirs(out, exist_ok=True)
    for name in ("talent_insights.ipynb", "explore_lake.ipynb"):
        with open(os.path.join(project, "notebooks", name)) as handle:
            notebook = json.load(handle)
        for cell in notebook["cells"]:
            if cell["cell_type"] == "code":
                cell["outputs"], cell["execution_count"] = [], None
        with open(os.path.join(out, name), "w") as handle:
            json.dump(notebook, handle, indent=1)

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for package in sorted(os.listdir(project)):
            if package == "tests" or not os.path.isfile(os.path.join(project, package, "__init__.py")):
                continue
            for folder, dirs, files in os.walk(os.path.join(project, package)):
                dirs[:] = sorted(d for d in dirs if d != "__pycache__")
                for name in sorted(f for f in files if f.endswith(".py")):
                    path = os.path.join(folder, name)
                    archive.write(path, os.path.relpath(path, project))
    with open(os.path.join(out, "package.zip.b64"), "w") as handle:
        handle.write(base64.b64encode(buffer.getvalue()).decode())

    with open(os.path.join(project, "requirements.txt")) as handle:
        requirements = handle.read().rstrip("\n")
    with open(os.path.join(out, "requirements.txt"), "w") as handle:
        handle.write(f"{requirements}\n# charts in the notebook (the image's matplotlib is built for NumPy 1)\nmatplotlib==3.11.2\n")
    with open(os.path.join(out, "setup.sh"), "w") as handle:
        handle.write(SETUP)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
