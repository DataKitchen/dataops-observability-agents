# Makefile for Sphinx documentation of DKUtils

.PHONY: \
	clean \
	help \
	Makefile


# Environment/global variables
SPHINXAPIDOC     = sphinx-apidoc
SPHINXAPIDOCOPTS = --force --module-first --separate --doc-project agents
MODULEPATH       = ../agents
EXCLUDEPATHS     = ../agents/tests

APIDOCDIR        = ./apidoc
APIDOCINDEX      = $(APIDOCDIR)/modules.rst

SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SPHINXPROJ    = observability-legacy-agents

SOURCEDIR     = .
BUILDDIR      = _build

# Local variables
pydeps = $(shell find $(MODULEPATH) -type f -name '*.py' -newer $(APIDOCINDEX))

# Utility targets
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

clean-apidoc:
	rm -rvf $(APIDOCDIR)

clean: clean-apidoc
	@IN_SPHINX=true $(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

# API documentation auto-compilation

$(APIDOCINDEX): $(pydeps)
	$(SPHINXAPIDOC) $(SPHINXAPIDOCOPTS) -o "$(APIDOCDIR)" "$(MODULEPATH)" "$(EXCLUDEPATHS)"

apidoc: $(APIDOCINDEX)

%.py: # Needed to prevent python files from going to catch-all target

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile $(APIDOCINDEX)
	@IN_SPHINX=true $(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
