## Latex Makefile for Window and Cygwin
include Makefile.common

imdm-cc-abstract.pdf:
	$(LATEX) imdb-cc-abstract < /dev/null
	$(BIBTEX) imdb-cc-abstract </dev/null 
	$(PDFLATEX) imdb-cc-abstract < /dev/null | $(SPAM_FILTER)
	$(PDFLATEX) imdb-cc-abstract < /dev/null| $(SPAM_FILTER)
	cp imdb-cc-abstract.pdf $(PAPER_TMSTAMP) ; echo $(PAPER_TMSTAMP)

