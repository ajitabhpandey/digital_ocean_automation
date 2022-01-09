#!/bin/bash
#
# Digital Ocean Load Balancer Resize
#

function print_usage() {
  echo "Usage: $0 LB_NAME NEW_SIZE"
  echo "Example:"
  echo "$0 mylbname 4"
  echo "$0 mylbname 1"
  return 0
}

function print_lb_config() {
  local LB_NAME=$1
  [[ -z ${LB_NAME} ]] && { echo "Function print_lb_config: Expecting load balancer name as argument";exit 1; }

  doctl compute load-balancer list ${LB_NAME} --format ID,Name,SizeUnit,Region,Status
}

function validate_lb_size() {
  local LB_NAME=$1
  [[ -z ${LB_NAME} ]] && { echo "Function validate_lb_size: Expecting load balancer name as argument";return 1; }

  local NEW_SIZE=$2
  [[ -z ${NEW_SIZE} ]] && { echo "Function validate_lb_size: Expecting size unit as argument";return 1;}

  local CURRENT_SIZE=$(doctl compute load-balancer list ${LB_NAME} --format SizeUnit --no-header)

  if [ ${CURRENT_SIZE} == ${NEW_SIZE} ]
  then
     echo "New size is same as current size"
     return 1
  fi

  return 0
}

function main() {
  
  local LB_ID=$1
  [[ -z ${LB_ID} ]] && { echo "Function main: Expecting load balancer id as argument"; return 1; }

  local NEW_SIZE=$2
  [[ -z ${NEW_SIZE} ]] && { echo "Function main: Expecting new size for load balancer in units as argument"; return 1; }

  echo "Current load balancer Config..."
  print_lb_config ${LB_NAME}

  # Get LB Current Settings
  read -r FORWARDING_RULES REGION TAG HTTP_REDIRECT_STATUS HEALTH_CHECK <<< "$(doctl compute load-balancer list ${LB_NAME} \
    --format ForwardingRules,Region,Tag,RedirectHttpToHttps,HealthCheck --no-header)"
  [[ -z ${FORWARDING_RULES} || -z ${REGION} || -z ${TAG} || -z ${HTTP_REDIRECT_STATUS} || -z ${HEALTH_CHECK} ]] \
    && { echo "Function main: Current LB Settings could not be obtained"; return 1; }

  # Resize LB
  echo "Resizing Load Balancer ${LB_NAME} - ${LB_ID} to ${NEW_SIZE} units"
  doctl compute load-balancer update ${LB_ID} \
    --name ${LB_NAME} --forwarding-rules ${FORWARDING_RULES} \
    --region ${REGION} --tag-name ${TAG} --redirect-http-to-https ${HTTP_REDIRECT_STATUS} \
    --health-check ${HEALTH_CHECK} --size-unit ${NEW_SIZE}

  # List the resized
  echo "The resized load balancer Config..."
  print_lb_config ${LB_NAME}
}
###
#
###
LB_NAME=$1
[[ -z ${LB_NAME} ]] && { echo "Expecting load balancer name as argument"; print_usage; exit 1; }

NEW_SIZE=$2
[[ -z ${NEW_SIZE} ]] && { echo "Expecting new size for load balancer in units as argument"; print_usage; exit 1; }

# Validate the current size
if ! validate_lb_size ${LB_NAME} ${NEW_SIZE}
then
  exit 1
fi

# Find the droplet id of the given host name
LB_ID=$(doctl compute load-balancer list ${LB_NAME} --format ID --no-header)
[[ -z ${LB_ID} ]] && { echo "Could not find load balancer id."; exit 1; }

main ${LB_ID} ${NEW_SIZE}
exit
