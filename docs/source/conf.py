import os
import sys
current_dir = os.path.dirname(__file__)
target_dir = os.path.abspath(os.path.join(current_dir, "../../"))
sys.path.insert(0, target_dir)


# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Andes -> Projet Mollusque'
copyright = '2023, David Sean-Fortin'
author = 'David Sean-Fortin'
release = '1'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
                'sphinx.ext.autodoc',
                'sphinx.ext.autosummary',
                'sphinx.ext.napoleon',
                'sphinx.ext.viewcode',
                'myst_parser'
             ]

templates_path = ['_templates']
exclude_patterns = []



html_theme = 'furo'
html_static_path = ['_static']
