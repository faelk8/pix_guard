from datetime import datetime
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

project = "PixGuard AI"
author = "Rafael Batista"
release = "0.1.0"
ano_inicio = 2026
ano_atual = datetime.now().year
copyright = (
    f"{ano_inicio}, {author}"
    if ano_atual == ano_inicio
    else f"{ano_inicio} – {ano_atual}, {author}"
)

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinxcontrib.autodoc_pydantic",
    "sphinxcontrib.mermaid",
    "myst_parser",
    "sphinx_copybutton",
]
templates_path = ["_templates"]
exclude_patterns = ["_build", "build", "Thumbs.db", ".DS_Store"]
source_suffix = {".rst": "restructuredtext", ".md": "markdown"}
master_doc = "index"
language = "pt_BR"

myst_enable_extensions = ["colon_fence", "deflist", "tasklist"]
myst_heading_anchors = 3
myst_fence_as_directive = {"mermaid"}

html_theme = "pydata_sphinx_theme"
html_title = "PixGuard AI · Documentação"
html_theme_options = {
    "logo": {"text": "PixGuard AI Docs"},
    "navbar_start": ["navbar-logo"],
    "navbar_center": [],
    "navbar_end": [],
    "navbar_persistent": ["search-button-field"],
    "show_nav_level": 2,
    "navigation_depth": 4,
    "show_toc_level": 2,
    "show_prev_next": True,
    "collapse_navigation": True,
    "secondary_sidebar_items": ["page-toc"],
    "article_header_end": ["sidebar-secondary-toggle"],
    "footer_start": ["copyright"],
    "footer_end": ["sphinx-version", "theme-version"],
}
html_context = {"default_mode": "dark"}
html_static_path = ["_static"]
html_sidebars = {"index": [], "**": ["sidebar-collapse", "sidebar-nav-bs"]}
mermaid_params = ["--theme", "forest"]

autodoc_pydantic_model_show_json = True
autodoc_pydantic_settings_show_json = True
autodoc_pydantic_model_show_config_summary = False

copybutton_prompt_text = r">>> |\.\.\. |\$ |In \[\d+\]: |\[\d+\]: "
copybutton_prompt_is_regexp = True


def setup(app):
    """Registra o estilo e o controle das barras laterais."""
    app.add_css_file("custom.css")
    app.add_js_file("sidebar-controls.js", defer="defer")
