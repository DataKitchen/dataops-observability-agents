# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

sys.path.insert(0, os.path.abspath(".."))


# -- Project information -----------------------------------------------------

project = "Observability Legacy Agents"
copyright = "2022, DataKitchen"
author = "DataKitchen"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.napoleon",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.autosummary",
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",  # https://www.sphinx-doc.org/en/master/usage/extensions/viewcode.html
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

# -- Options for autodoc extension -------------------------------------------

# group special methods & non special methods, rather than alphabetical
autodoc_member_order = "groupwise"

# pull signatures for C functions as needed
autodoc_docstring_signature = True

# for classes, show docstrings for class and class.__init__()
autoclass_content = "both"

autodoc_default_flags = [
    "show-inheritance",
    "undoc-members",
    "special-members",
    # 'private-members',
    # 'inherited-members',
]

# Never document these methods/attributes
autodoc_exclude_always = {
    "__class__",
    "__delattr__",
    "__dict__",
    "__format__",
    "__hash__",
    "__module__",
    "__new__",
    "__reduce__",
    "__reduce_ex__",
    "__sizeof__",
    "__slots__",
    "__subclasshook__",
    "__weakref__",
}

# Document these only if their docstrings don't match those of their base classes
autodoc_exclude_if_no_redoc_base_classes = [object, type]
autodoc_exclude_if_no_redoc = {
    "__init__",
    "__getattribute__",
    "__setattribute__",
    "__getattr__",
    "__setattr__",
    "__getitem__",
    "__setitem__",
    "__str__",
    "__repr__",
    "__iter__",
    "next",
    "__next__",
    "close",
    "__del__",
}

# -- Options for napoleon extension ------------------------------------------

napoleon_google_docstring = False
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = True
napoleon_use_param = True
napoleon_use_rtype = True
