
export APP_HOME:=$(PWD)

export APP_NAME=deduplifier
export APP_PATH=/app

SUBDIRS=code

.PHONY: $(SUBDIRS)

build: $(SUBDIRS)

$(SUBDIRS):
	cd $@ && $(MAKE) $(TARGET)

