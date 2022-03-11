
CODE_PATH=$(APP_PATH)/code

include $(TOP)/Makefile.common

## new-vm definitions

APP_HOST=$(APP_NAME)-host

THIRD_PARTY_DIR=$(TOP)/$(APP_NAME)/third-party-stuff

APP_OS_VER=20.04
APP_OS_NAME=ubuntu$(APP_OS_VER)
#APP_OS_ISO=/debian-9.0.0-amd64-netinst.iso

CPUS=1
# memory in MiB
MEM=8132
# disk size in MiB
DISK_SIZE=40
MP_DISK_SIZE=$(DISK_SIZE)g
DISK_PATH=

MP_IMAGE_NAME=$(APP_OS_VER)

HOST_VM_CLOUD_INIT_FILE=host-vm-cloud-config.yml
CLOUD_INIT_FILE=cloud-config.yml

FORCE_VM_CREATION=0
FORCE_CONTAINER_CREATION=0

# these should match the definitions in the other config files (and should
# actually be extracted from them dynamically but...)
SERVICES=web redis
SERVICE_PROTO=tcp

# WIP - at some point we could need multiple ports exposed, but let's ignore that for now...
WEB_EXTERNAL_PORT=$(DEDUPLIFIER_EXTERNAL_PORT)
WEB_INSTANCE_PORT=5000

DB_PATH=$(APP_PATH)/db

DB_NAME=$(APP_NAME)
DB_NAME_TEST=dd_test0

TEST_DIR=$(TOP)/$(APP_NAME)/test

DB_USER=postgres
DB_PASSWORD=$(DB_USER)

#DB_HOST=127.0.0.1
# this will be evaluated late, at time-of-use, which should be in Makefile.post,
# after HOST_VM_IP is set.
DB_HOST=$(HOST_VM_IP)

DB_EXTERNAL_PORT=6681
DB_INSTANCE_PORT=3368

WEB_ALLOWED_HOSTS=127.0.0.1,10.0.0.33
DB_ALLOWED_HOSTS=$(WEB_ALLOWED_HOSTS)

DB_MAX_CONNECTIONS=1000

DB_ARCHIVE_DIR=$(TOP)/$(APP_NAME)/archive
DB_DUMP_FILE=$(DB_ARCHIVE_DIR)/$(RUNID)-dump

.PHONY=build

build: $(CREATE_DOCKER_HOST_VM) $(CREATE_DOCKER_CONTAINERS) $(CREATE_DB)

include $(TOP)/Makefile.post

