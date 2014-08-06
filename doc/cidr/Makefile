## Latex Makefile for Window and Cygwin
include Makefile.common

# These following rules assume a Cygwin environment with a .eps
# printer set as the default. The HP LaserJet 2300 PS is a good one to
# use (driver ships w/ Windodws). Install it as print-to-file, set its
# postscript options to emit EPS, and you should be good to go.
%.prn: figure-sources/%.doc
	cygstart -p -d $(*D) $<

%.prn: figure-sources/%.docx
	cygstart -p -w -d $(*D) $<

%.prn: figure-sources/%.ppt
	cygstart -p -w -d $(*D) $<

%.prn: figure-sources/%.pptx
	cygstart -p -w -d $(*D) $<

%.prn: figure-sources/%.pdf.in
	cp $< $*.pdf
	cygstart -p -d $(*D) $*.pdf


%.eps: %.eps.in
	cp figure-sources/$*.eps.in $*.eps

#IP: the -w parameter is not working with Office2003.
%.eps: %.prn
	./prn2eps $< $@

# %.eps: %.jgr
# 	jgraph $< > $@

#%.eps: %.pdf
#	pdftops -eps $< $@

#not used anymore
#format:
#	sort -t' ' -k18,18 -n gccResults.factBig2B.times  | sort -t' ' -k14,14 -n | sort -t' ' -k20,20 | awk -F' ' '{print $1, $4, ($4-$22), $22}' > gccResults.factBig2B.times.op


## From: http://ghostscript.com/doc/current/Ps2pdf.htm
## To disable the automatic orientation selection algorithm use: -dAutoRotatePages=/None
##
## When there is no text on the page or automatic page rotation is set to /None 
## an orientation value from setpagedevice is used. 
## Valid values are: 0 (portrait), 3 (landscape), 2 (upside down), and 1 (seascape). 
## The orientation can be set from the command line as -c "<</Orientation 3>> setpagedevice" 
## using Ghostscript directly but cannot be set in ps2pdf.
##	ps2pdf -dEPSCrop -dAutoRotatePages=/None $< $@
%.pdf: %.eps
	ps2pdf -dEPSCrop $< $@
	cp $*.pdf figure-sources/$*.pdf.in

%.dvi: %.tex ../common/biblio.bib Makefile
	$(LATEX) $* < /dev/null
	bibtex $* </dev/null 
	$(LATEX) $* < /dev/null
	$(LATEX) $* < /dev/null

%.ps: %.dvi
	dvips -Ppdf -t letter $*

.PRECIOUS: %.eps %.pdf %.dvi

SPAM_FILTER = awk '$$0 ~ /^Warning: pdflatex/ {moreskip=2; next} moreskip > 0 { moreskip--; next } {print}'
%.pdf:: %.tex ../common/biblio.bib Makefile.common Makefile
	$(LATEX) $* < /dev/null
	bibtex $* </dev/null 
	pdflatex $* < /dev/null | $(SPAM_FILTER)
	pdflatex $* < /dev/null| $(SPAM_FILTER)
	cp $*.pdf $(PAPER_TMSTAMP) ; echo $(PAPER_TMSTAMP)


dated: %.pdf
	cp $*.pdf $(PAPER_TMSTAMP) ; echo $(PAPER_TMSTAMP)


dated-ps: %.ps
	cp $*.ps $(PAPERPS_TMSTAMP) ; echo $(PAPERPS_TMSTAMP)
