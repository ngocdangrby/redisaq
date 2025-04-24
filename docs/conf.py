import os
import sys

sys.path.insert(0, os.path.abspath(".."))

project = "Redisq"
copyright = "2025, Do Quoc Vuong"
author = "Do Quoc Vuong"
release = "0.1.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "alabaster"
html_static_path = ["_static"]
