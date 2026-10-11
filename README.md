# cibc-exercise

## Environment Setup
The solution has been developed against Python 3.12 and Pyspark 3.5.4.
To run the code create a Python virtual environment, e.g. from a command prompt run: 

`conda create -n cibc-exercise python=3.12`

`conda activate cibc-exercise`

then change the current directory to be the project root directory and install the required modules by running:

`pip install -r ./requirements.txt`

This is everything required for the solution to run.

## Project Structure

Here is a description of the top level directories:
- config - Contains application.yaml configuration file with properties defining the different locations where data is being stored
- data - This is the parent folder for data storage. Underneath it there are directories representing the different stages of the data travelling through the medallion architecture
- data/landing - The input files available for ingestion are located here
- data/bronze - Contains data belonging to the Bronze layer
- data/silver - Contains data belonging to the Silver layer
- data/corrupt - This is where the corrupt records are getting stored
- data/stats - Contains information about the different pipeline runs and some statistics, e.g. good/bad record counts, duplicates, etc.
- jars - JAR files required for operating with Delta Tables
- notebooks - Jupyter Notebooks where the different pipelines can be executed from
- src - The Python code of the actual solution is contained in this folder
- src/jobs/bronze - Ingestion jobs are located in this folder
- src/jobs/silver - The transformation jobs are placed here
- src/jobs/gold - This is where the report generator resides
- tests - Contains the unit tests of the solution

## Running the Test

Open a terminal window and activate the Python virtual environment of the project, i.e. cibc-exercise.
Change the current directory to the project root directory.

!!! In case there is already a Spark installation on the machine unset SPARK_HOME environment variable to prevent any conflicts by running:

`unset SPARK_HOME`

 and then run the command:

`pytest`


## Running the Pipelines

Open a terminal window and activate the Python virtual environment of the project, i.e. cibc-exercise.
Change the current directory to the project root directory.

!!! In case there is already a Spark installation on the machine unset SPARK_HOME environment variable to prevent any conflicts by running:

`unset SPARK_HOME`

 and then run the command:

`jupyter notebook`

This will start the Jupyter server and will open the UI interface in a browser.
Navigate to the **notebooks** folder and click on the pipelines.ipynb notebook to open it.
Follow the instructions in the notebook for running the individual pipelines.

## Assumptions and Limitations

Some design and implementation decisions and trade-offs have been made to keep the solution
simple and to be able to fit within the allocated time. These include:

- There is currently no proper error handling. If an error occurs during the processing the pipeline will fail and there will be no record created in the Stats table
- There is only minimum logging that will not provide enough details for troubleshooting in a real Prod system
- The solution currently stores the latest version of each trade in Silver layer. In a Production system all versions of the trades would have to be saved with proper effective_from and effective_to timestamps
- Because of the way trades are stored (only latest version) the PnL report will not be accurate if produced for dates prior to the trade date
- The solution assumes that all the trades are for long positions and that there are no short selling transactions
- The schema enforcement in the Transformation jobs is only at very basic level and the rules are embedded in the code. In a proper system these rules would be externalised and stored either in separate rules/schema files or in a centralised system.
- The unit tests provide only basic coverage. In a Production system there would be much higher test coverage and assertions
- There is certain amount of code duplication in transformation jobs that can be avoided by extracting common functionality in a parent class
- The Stats collection mechanism is very basic one. More comprehensive monitoring and stats collection can be implemented in a real system.
- This solution only tracks a small number of DQ metrics, e.g. number of corrupt records and number of duplicates
