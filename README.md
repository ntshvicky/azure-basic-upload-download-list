
# create virtual environment
python3 -m virtualenv env 

# activate virtualenv
source env/bin/activate

# install all packages
python3 -m pip install -r requirements.txt

# add details to .env file

#clear cache
find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf