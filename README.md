### Crime Analysis of Cities in the USA
#### Data Extraction and Pre-processing
This program is to clean and analyze the data from the 5 datasets.
- [Austin.csv](https://data.austintexas.gov/Public-Safety/Crime-Reports/fdj4-gpfu)
- [Baltimore.csv](https://data.baltimorecity.gov/Public-Safety/BPD-Part-1-Victim-Based-Crime-Data/wsfq-mvij)
- [Chicago.csv](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2)
- [LACity.csv](https://data.lacity.org/A-Safe-City/Crime-Data-from-2010-to-2019/63jg-8b9z)
- [Rochester.csv](https://data-rpdny.opendata.arcgis.com/datasets/rpd-part-i-crime-2011-to-present)

Download the datasets from above links and rename the datasets by their city name. 
There should be 5 CSV files: Austin.csv, Baltimore.csv, Chicago.csv, LACity.csv, Rochester.csv

#### How to run the program? 
- Open the config folder.
- Set database name, hostname and port number in the connection.json file
- Set the base path containing the csv files in config/path.json
- Open the terminal and run the following commands<br />
    * To load the data to MongoDB: `$ python3 preprocess.py`

    * For visualizations: `$ python3 visualize.py`
    
    * For association rule mining: `$ python3 associationrulemining.py`
    
    * For classification: `$ python3 Classification.py`


### Packages Required:
- mlxtend
- numpy
- pandas
- scipy
- xgboost
- libomp

Please maintain the following directory structure:
- src
    * `associationrulemining.py`
    * `Classification.py`
    * `connection.py`
    * `offense_type.json`
    * `preprocess.py`
    * `visualize.py`
- config
    * `connection.json`
    * `path.json`
    
- Datasets
    * place the downloaded datasets here and change the file path in `path.json`
    
