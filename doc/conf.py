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
sys.path.insert(0, os.path.abspath('..'))


# -- Project information -----------------------------------------------------

project = 'mpikat'
copyright = '2020, The mpikat developers'
author = 'The mpikat developers'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ['sphinx.ext.autodoc', 'sphinx.ext.coverage',
        'sphinx.ext.napoleon', 'sphinx_rtd_theme',  'recommonmark', 'sphinx.ext.intersphinx']
# 'sphinx.ext.viewcode',

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


#apidoc_module_dir = '../mpikat'
#apidoc_output_dir = '_reference'
#apidoc_excluded_paths = ['tests', '*test*']
#apidoc_separate_modules = True
#apidoc_module_first = True


#AUTOAPI
extensions.append('autoapi.extension')

autoapi_type = 'python'
autoapi_dirs = ["../mpikat", "../scripts"]
autoapi_add_toctree_entry = False
autoapi_options = ["members", "undoc-members", "show-inheritance-diagram", "show-inheritance", "imported-members"]
autoapi_python_use_implicit_namespaces = True
#"show-module-summary"

autoapi_ignore = ['*test*', '*pynvml.py']
autoapi_python_class_content = 'both'
autoapi_keep_files = True


#########################################################################
# Intersphinx
########################################################################

intersphinx_mapping = {
        'katcp': ('https://pythonhosted.org/katcp/', None),
        'edd': ('http://mpifr-bdg.pages.mpcdf.de/edd_documentation/', None),
        'psrdada_cpp': ('http://mpifr-bdg.pages.mpcdf.de/psrdada_cpp/', None),
        }



# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

html_theme_options = {
        'collapse_navigation': False,
#        'canonical_url': "https:// ... /",
        'logo_only': True,
        'display_version': False,
        }

html_show_sourcelink = False

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
