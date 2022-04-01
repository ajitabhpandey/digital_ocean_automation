#!/bin/bash
#
# Digital Ocean Droplet resize
#

function print_usage() {
  echo "Usage: $0 DROPLET_NAME NEW_SIZE"
  echo "Example:"
  echo "$0 mydroplet_name.fqdn c-8"
  echo "$0 mydroplet_name.fqdn s-1vcpu-1gb"
  echo "$0 mydroplet_name.fqdn s-1vcpu-1gb-amd"
  echo "$0 mydroplet_name.fqdn s-2vcpu-2gb-amd"
  return 0
}

function wait_for_task() {
  local DROPLET_ID=$1
  [[ -z ${DROPLET_ID} ]] && { echo "Function wait_for_task: Expecting droplet id as argument";return 1; }

  local TASK_ID=$2
  [[ -z ${TASK_ID} ]] && { echo "Function wait_for_task: Expecting task id as argument";return 1; }

  while true
  do
    local TASK_STATUS=$(doctl compute droplet-action get ${DROPLET_ID} --action-id ${TASK_ID} \
      |grep -v 'ID'|awk '{print $2}')
    
    echo "TASK Status is: ${TASK_STATUS}"

    if [[ ${TASK_STATUS} == 'completed' ]]
    then
      return 0
    else
      sleep 10s
    fi
  done
}

function validate_size() {
  local DROPLET_NAME=$1
  [[ -z ${DROPLET_NAME} ]] && { echo "Function validate_size: Expecting droplet name as argument";return 1;}

  local NEW_SIZE=$2
  [[ -z ${NEW_SIZE} ]] && { echo "Function validate_size: Expecting droplet size as argument";return 1;}

  local CURRENT_SIZE=$(doctl compute droplet list ${DROPLET_NAME} --output json|awk -F\" '/size_slug/{print $4}')

  if [ ${CURRENT_SIZE} == ${NEW_SIZE} ]
  then
     echo "New size is same as current size"
     return 1
  fi

  return 0
}

function poweroff_droplet() {
  local DROPLET_ID=$1
  [[ -z ${DROPLET_ID} ]] && { echo "Function poweroff_droplet: Expecting droplet id as argument";return 1; }

  # Check droplet status
  local DROPLET_STATUS=$(doctl compute droplet get ${DROPLET_ID} --template "{{ .Status}}")
  [[ -z ${DROPLET_STATUS} ]] && { echo "Function poweroff_droplet: Could not obtain droplet status";return 1; }
  
  echo "DROPLET Status is: ${DROPLET_STATUS}"
  
  if [[ ${DROPLET_STATUS} == "off" ]]
  then
    echo "Droplet status is off. Doing nothing."
    return 0
  elif [[ ${DROPLET_STATUS} == "active" ]]
  then
    echo "Droplet status is active. Proceeding with power-off."
    local TASK_ID=$(doctl compute droplet-action power-off ${DROPLET_ID} \
      |grep -v 'ID'|awk '{print $1}')

    echo "Poweroff Task ID is: ${TASK_ID}"

    if wait_for_task ${DROPLET_ID} ${TASK_ID}
    then
      return 0
    else
      echo "Something went wrong while monitoring task_status, please manually check."
      return 1
    fi
  else
    echo "Unknown status of droplet. Doing nothing."
    return 1
  fi
}

function poweron_droplet() {
  local DROPLET_ID=$1
  [[ -z ${DROPLET_ID} ]] && { echo "Function poweron_droplet: Expecting droplet id as argument";return 1; }

  local TASK_ID=$(doctl compute droplet-action power-on ${DROPLET_ID} \
    |grep -v 'ID'|awk '{print $1}')
  
  echo "Poweron Task ID is ${TASK_ID}"

  if wait_for_task ${DROPLET_ID} ${TASK_ID} 
  then
    # Verify that the droplet is active
    local DROPLET_STATUS=$(doctl compute droplet get ${DROPLET_ID} --template "{{ .Status}}")

    echo "Droplet Status is: ${DROPLET_STATUS}"
  
    if [[ ${DROPLET_STATUS} == "active" ]]
    then
      return 0
    else
      echo "Something went wrong. Droplet status is not active after power-on."
      return 1
    fi

  fi
}

function resize_droplet() {
  local DROPLET_ID=$1
  [[ -z ${DROPLET_ID} ]] && { echo "Function resize_droplet: Expecting droplet id as argument";return 1; }

  local NEW_SIZE=$2
  [[ -z ${NEW_SIZE} ]] && { echo "Function resize_droplet: Expecting new size as argument";return 1; }

  local TASK_ID=$(doctl compute droplet-action resize ${DROPLET_ID} --size ${NEW_SIZE} \
    |grep -v 'ID'|awk '{print $1}')
  
  echo "Resize Task ID is ${TASK_ID}"
  
  if wait_for_task ${DROPLET_ID} ${TASK_ID}
  then
    return 0
  fi
  
}

function main() {
  
  local DROPLET_ID=$1
  [[ -z ${DROPLET_ID} ]] && { echo "Function main: Expecting droplet id as argument"; return 1; }

  local NEW_SIZE=$2
  [[ -z ${NEW_SIZE} ]] && { echo "Function main: Expecting new size as argument"; return 1; }

  # Power Off the Droplet
  echo "Powering off droplet ${DROPLET_NAME} - ${DROPLET_ID}"
  poweroff_droplet ${DROPLET_ID}

  # Resize Droplet to desired size
  echo "Resizing ${DROPLET_NAME} - ${DROPLET_ID} to ${NEW_SIZE}"
  resize_droplet ${DROPLET_ID} ${NEW_SIZE}

  # Power On the Droplet
  echo "Powering on droplet ${DROPLET_NAME} - ${DROPLET_ID} "
  poweron_droplet ${DROPLET_ID}

  # List the resized
  echo "The resized droplet is..."
  doctl compute droplet list ${DROPLET_NAME} \
    --format ID,Name,PublicIPv4,Memory,VCPUs,Disk,Region,Status
}
###
#
###
DROPLET_NAME=$1
[[ -z ${DROPLET_NAME} ]] && { echo "Expecting droplet name as argument"; print_usage; exit 1; }

NEW_SIZE=$2
[[ -z ${NEW_SIZE} ]] && { echo "Expecting new size as argument"; print_usage; exit 1; }

# Validate the current size
if ! validate_size ${DROPLET_NAME} ${NEW_SIZE}
then
  exit 1
fi

# Find the droplet id of the given host name
DROPLET_ID=$(doctl compute droplet list ${DROPLET_NAME} --no-header --format "ID")
[[ -z ${DROPLET_ID} ]] && { echo "Could not find droplet id."; exit 1; }

main ${DROPLET_ID} ${NEW_SIZE}
exit
