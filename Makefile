all: kuboribs gwcfg s3-proxy

kuboribs:
	go build -o kuri ./integrations/kuri/cmd/kuri
.PHONY: kuboribs

gwcfg:
	go build -o gwcfg ./integrations/gwcfg
.PHONY: gwcfg

s3-proxy:
	go build -o s3-proxy ./server/s3frontend/cmd
.PHONY: s3-proxy

clean:
	rm -f kuri gwcfg s3-proxy
.PHONY: clean