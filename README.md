# digital_ocean_automation

Various scripts, playbooks etc for managing digital ocean platform. Below is a brief information and requirement about each of these scripts.

## Shell Scripts based on doctl

These scripts are based on the command line client provided by Digital Ocean - [doctl](https://github.com/digitalocean/doctl). Refer [this article](https://www.digitalocean.com/community/tutorials/how-to-use-doctl-the-official-digitalocean-command-line-client) for more details on how to setup and use ```doctl```. The official documentation for ```doctl``` is [here](https://docs.digitalocean.com/reference/doctl/)

These were written to help me to use the resources in an elastic fashion as auto-scaling was not available at the time of writing these. Just type the script name and it will print out the usage syntax.

**droplet_resize.sh** - Resizes a specified droplet. This is useful to resize the droplet to a higher CPU/Memory when needed and reduced to a lower CPU/Memory during lean hours. This will help contain the cost.

**lb_resize.sh** - Resizes a load balancer. This script is based on the newer load balancer offering wherein we can specify size in units - each unit with a certain capacity. During peak time the LB can be resized with higher number of units and during lean times the number of LB units can be reduced to save the cost. I use LB with tags, so all droplets with a specified tag will be added to the LB automatically. This is a resize script, so other than the size units, I have reused all the current values while updating the LB.

## Python Scripts

**upload2spaces.py** - Uploads all files in a specified local directory to a bucket in spaces. Currently caters to a selected set of files (flv and mp4) as specified in the code.

### Required Environment Variables

Either these variables can be set at the operating system level or a _.env_ file can be created.

**DO_ACCESS_ID** - Your Digital Ocean Access Key

For example,
DO_ACCESS_ID = 'sjdSDs726762374nsjdhdw8283HJHF'

**DO_SECRET_KEY** - Your Digital Ocean Secret Key

For example,
DO_SECRET_KEY = 'ADJndsjd83267697Kgdswm8752328724100B'

**DO_REGION** - Your Digital Ocean Region where you have your bucket created

For example,
DO_REGION = 'sgp1'

#### Environment variables required Specifically for Spaces related scripts

**DO_BUCKET** - Your Digital Ocean bucket name

For example,
DO_BUCKET = 'backup1'

**DO_TARGET_FOLDER** - The target folder in the bucket where the files will be uploaded. Trailing slash is mandatory. Path can be specified as shown in the second example below. If trailing slash is missing, it will be added.

For example,
DO_TARGET_FOLDER = 'Movies/'
or
DO_TARGET_FOLDER = 'Movies/Set1/'

**LOCAL_SOURCE_DIR** - The source directory on the local filesystem from where the files will be uploaded

For example,
LOCAL_SOURCE_DIR = '/home/ubuntu/Movies/'

### Deployment

Easiest way is to use a virtual environment. The following set of commands will build the required virtual environment.

```bash
python3 -m venv venv
source venv/bin/activate
python3 -m pip install --upgrade pip
pip install -r requirements.txt
deactivate
```

In order to execute the scripts, the virtual environment needs to be activated and once the script execition is completed it needs to be deactivated. The following one liner will do it all together.

```bash
(cd /opt/digital_ocean_automation/;source /opt/digital_ocean_automation/venv/bin/activate && /opt/digital_ocean_automation/venv/bin/python3 /opt/digital_ocean_automation/upload2spaces.py;deactivate)
```
