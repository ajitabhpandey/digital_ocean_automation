# digital_ocean_automation

Various scripts, playbooks etc for managing digital ocean platform.

**upload2spaces.py** - Uploads all files in a specified local directory to a bucket in spaces. Currently caters to a selected set of files (flv and mp4) as specified in the code.

## Required Environment Variables

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

### Environment variables required Specifically for Spaces related scripts

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
