all: kuboribs gwcfg

kuboribs:
	go build -o kuri ./integrations/kuri/cmd/kuri
.PHONY: kuboribs

gwcfg:
	go build -o gwcfg ./integrations/gwcfg
.PHONY: gwcfg
clean:
	rm -f kuri gwcfg