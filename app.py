
from azure_bucket import download_container_contents

# download container contents from Azure bucket in local directory
local_directory = 'downloads/'
download_container_contents(local_directory)