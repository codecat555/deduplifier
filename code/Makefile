
include $(TOP)/Makefile.common

# path on host vm where current directory is mounted
CODE_PATH=$(APP_PATH)/code
DB_PATH=$(APP_PATH)/db

## new-vm definitions

APP_HOST=$(APP_NAME)-host

THIRD_PARTY_DIR=$(TOP)/$(APP_NAME)/third-party-stuff

APP_OS_VER=20.10
APP_OS_NAME=ubuntu$(APP_OS_VER)
#APP_OS_ISO=/debian-9.0.0-amd64-netinst.iso

CPUS=1
# memory in MiB
MEM=8132
# disk size in MiB
DISK_SIZE=10
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
WEB_INSTANCE_PORT=$(WEB_EXTERNAL_PORT)

DB_USER=postgres
DB_PASSWORD=$(DB_USER)

# this value dictated by postgres (by the docker image we're using)
DB_EXTERNAL_PORT=6881
DB_INSTANCE_PORT=3368

.PHONY=build

build: $(CREATE_DOCKER_HOST_VM) $(CREATE_DOCKER_CONTAINERS)

include $(TOP)/Makefile.post

