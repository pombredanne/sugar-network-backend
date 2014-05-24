VERSION = $(shell grep ^version sweets.recipe | awk '{print $$3}')
STABILITY = $(shell grep ^stability sweets.recipe | awk '{print $$3}')

all: sugar_network/toolkit/languages.py sugar_network/__init__.py

sugar_network/toolkit/languages.py: sugar_network/toolkit/languages.py.in
	cp $< $@
	langs=$$(for i in `ls po/*.po`; do echo -n "'`basename $$i .po`',"; done); sed -i "s/%LANGUAGES%/$$langs/" $@

sugar_network/__init__.py: sugar_network/__init__.py.in
	cp $< $@
	sed -i "s/%VERSION%/$(VERSION)-$(STABILITY)/" $@

.PHONY: all
