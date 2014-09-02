How to produce documentation using Sphinx
=========================================

If there is no change in the list of modules:

* Fix sys.path.insert in docs/conf.py, by adding the paths that must be included in PYHTONPATH to import all modules needed by those to be documented.

* Run:

::

   cd docs
   make html

This will try to build all HTML content in html directory under BUILDDIR, as defined in docs/Makefile (which should exist). Therefore, change that variable to your taste.

If there are changes in the list of modules:

::

   cd docs
   sphinx-apidoc --force --separate -o . ..
   make html

How to push documentation to GitHub
===================================

The directory where the HMTL content was built should be a clone of the gh-pages branch of the project. So, first clone the project, change to the gh-pages branch, and then run the above commands to populate with the new HTML files. Remember that the HTML files will be placed under the html directory of the build dir (as specified in the Makefile), so it makes sense to clone the project as html subdir. Eg, if the build dir is GrimoireLib-docs:

::

   mkdir GrimoireLib-docs
   git clone https://github.com/VizGrimoire/GrimoireLib.git GrimoireLib-docs/html
   cd GrimoireLib-docs/html
   git checkout gh-pages

Now, run the doc generation commands in the GrimoireLib/docs directory (GrimoireLib is the directory where the GrimoireLib repository is cloned, and the commands are those detailed above).

After that:

::

   git add .
   git commit -m "Updating HTML docs" 
   git push origin gh-pages

More information about documenting with Sphinx
==============================================

* `reStructuredText Primer <http://sphinx-doc.org/rest.html>`_

* `Publishing sphinx-generated docs on github <http://daler.github.io/sphinxdoc-test/includeme.html>`_
