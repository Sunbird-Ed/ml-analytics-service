# ml-analytics-service
#

#### Prerequisites
1. Python3.6.x or above
1. Python virtual environment to work with

#### Setup

Each folder consists of its own config files and programs. Navigate into respective folder and create a file with the name `config.ini` and copy the contents from `config.sample` present in the respective folders.

Replace the `<>` with appropriate values in the `config.ini` and then save.

Activate the virtual environment created earlier by running the following command `. <PATH-TO-YOUR-VIRTUAL-ENV-NAME>/bin/activate`

Run the following command to install all the dependencies `pip install -r requirements.txt`

#### Execution of Programs

For programs that start with `py_*` are a general ones. So use `python <PATH-TO-PROGRAM>`

For programs that start with `pyspark_*` use `spark-submit <PATH-TO-PROGRAM>`

### Execution Command to run Trust Review Active User Programs
python active_users.py --interval "2022-04-01T00:00:00+00:00/2022-05-01T00:00:00+00:00" --configPath "/opt/sparkjobs/ml-analytics-service_staging/trust_review"
