# CSPE64-Data-Analytics
This repository contains the code and data for the CSPE64 Data Analytics course at the National Institute of Technology, Tiruchirapalli. 

## Assignment 1

Steps to setup the environment:

1. Install Python 3.7 or above
2. Setup the environment using the following command:
```
virtualenv myenv
. myenv/bin/activate
```
3. Install the required packages using the following command:
```
pip install -r requirements.txt
```
4. Run the son_algorithm.py file using the following command:
```
python3 son_algorithm.py
```
5. The output will be updated in the Outputs directory.
6. To change the format of dataset for Toivenon Algo, run the following command:
```
python3 format_data.py > ./DataSet/transformed_data.csv
```
7. For Toivonen Algorithm, run the toivonen_algorithm.py file using the following command:
``` 
python3 toivonen_algorithm.py
```