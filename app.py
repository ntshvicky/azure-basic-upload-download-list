
import base64
import os
from azure_bucket import download_container_contents, upload_file_input

# download container contents from Azure bucket in local directory
local_directory = 'downloads/'
download_container_contents(local_directory)


# upload file from file upload option flask
def uploadFile(file):
    folder_path = 'uploads/'
    if os.path.exists(folder_path) == False:
        os.makedirs(folder_path)

    if file is not None:
        contents = file.read()
        image_path = os.path.join(folder_path, file.filename)
        with open(image_path, 'wb') as f:
            f.write(contents)

        _, cloud_path = upload_file_input(image_path, image_path.replace("uploads/", ''), file.mimetype)
        return cloud_path

    return None

# upload base64string
def uploadBase64(file, file_name):
    folder_path = 'uploads/'
    if os.path.exists(folder_path) == False:
        os.makedirs(folder_path)

    image_path = os.path.join(folder_path, file_name)
    base64Image = base64.decodebytes(file.encode())
    with open(image_path, "wb") as fh:
        fh.write(base64Image)
        fh.close() 

    _, cloud_path = upload_file_input(image_path, image_path.replace("uploads/", ''), file.mimetype)
    return cloud_path
