GOTOOLCHAIN ?= go1.25.7

all: kuboribs gwcfg s3-proxy ritool

kuboribs:
	GOTOOLCHAIN=$(GOTOOLCHAIN) go build -o kuri ./integrations/kuri/cmd/kuri
.PHONY: kuboribs

gwcfg:
	GOTOOLCHAIN=$(GOTOOLCHAIN) go build -o gwcfg ./integrations/gwcfg
.PHONY: gwcfg

s3-proxy:
	GOTOOLCHAIN=$(GOTOOLCHAIN) go build -o s3-proxy ./server/s3frontend/cmd
.PHONY: s3-proxy

ritool:
	GOTOOLCHAIN=$(GOTOOLCHAIN) go build -o ritool ./integrations/ritool
.PHONY: ritool

clean:
	rm -f kuri gwcfg s3-proxy ritool
.PHONY: clean
