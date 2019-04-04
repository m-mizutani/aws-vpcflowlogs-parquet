STACK_CONFIG ?= param.cfg

ifeq (,$(wildcard $(STACK_CONFIG)))
    $(error $(STACK_CONFIG) is not found)
endif

CODE_S3_BUCKET := $(shell cat $(STACK_CONFIG) | grep CodeS3Bucket | cut -d = -f 2)
CODE_S3_PREFIX := $(shell cat $(STACK_CONFIG) | grep CodeS3Prefix | cut -d = -f 2)
STACK_NAME := $(shell cat $(STACK_CONFIG) | grep StackName | cut -d = -f 2)
PARAMETERS := $(shell cat $(STACK_CONFIG) | grep -e LambdaRoleArn -e DstS3Region -e DstS3Bucket -e DstS3Prefix -e SrcS3Bucket -e SnsTopicArn -e AthenaTableName | tr '\n' ' ')
TEMPLATE_FILE=template.yml

all: deploy

test:
	go test -v

clean:
	rm build/main

build/main: *.go
	env GOARCH=amd64 GOOS=linux go build -o build/main

sam.yml: $(TEMPLATE_FILE) build/main
	aws cloudformation package \
		--template-file $(TEMPLATE_FILE) \
		--s3-bucket $(CODE_S3_BUCKET) \
		--s3-prefix $(CODE_S3_PREFIX) \
		--output-template-file sam.yml

deploy: sam.yml
	aws cloudformation deploy \
		--template-file sam.yml \
		--stack-name $(STACK_NAME) \
		--capabilities CAPABILITY_IAM \
		--parameter-overrides $(PARAMETERS)
