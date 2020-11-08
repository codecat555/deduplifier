
APP_NAME=deduplifier

APP_HOST=$(APP_NAME)-host

INSTALL_DIR=/app/$(APP_NAME)/
DEV_ROOT_DIR=/home/me/Documents/dev

THIRD_PARTY_DIR=$(DEV_ROOT_DIR)/$(APP_NAME)/third-party-stuff

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


new-vm:
	#virt-install --name=$(APP_NAME)-vm --vcpus=$(CPUS) --memory=$(MEM) --disk path=$(INSTALL_DIR),size=$(DISK_SIZE) --cdrom=$(APP_OS_ISO)
	-multipass delete $(APP_HOST)
	multipass purge
	multipass launch --verbose --cpus $(CPUS) -d $(MP_DISK_SIZE) --name $(APP_HOST) $(MP_IMAGE_NAME)
	-multipass exec $(APP_HOST) -- sudo apt-get remove docker docker-engine docker.io containerd runc
	multipass exec $(APP_HOST) -- sudo apt-get install apt-transport-https ca-certificates curl gnupg-agent software-properties-common
	multipass exec $(APP_HOST) -- curl -fsSL https://download.docker.com/linux/ubuntu/gpg | multipass exec $(APP_HOST) -- sudo apt-key add -
	multipass exec $(APP_HOST) -- sudo apt-key fingerprint 0EBFCD88
	multipass exec $(APP_HOST) -- sudo add-apt-repository --yes 'deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable'
	multipass exec $(APP_HOST) -- sudo apt-get update
	multipass exec $(APP_HOST) -- sudo apt-get --yes install docker-ce docker-ce-cli containerd.io
	multipass exec $(APP_HOST) -- sudo docker run hello-world 
	#multipass exec $(APP_HOST) -- sudo docker pull postgres

