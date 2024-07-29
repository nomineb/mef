# Electricity marginal emissions factor estimates project

## Setup and Dependencies
This code uses Python 3. To install all Python-related dependencies, use [environment.yml](https://github.com/nomineb/mef/blob/main/environment.yml) file. 

```bash
conda install git
git clone https://github.com/nomineb/mef.git
conda update conda
cd mef
conda env create -f environment.yml
conda activate mef
```

## Usage
The pipeline to generate formatted CEMS data, aggregated by different spatial options, run for the year of interest (2022 in this case):
```bash
cd src
python data_pipeline.py --year 2022
```
The downloaded raw data will be stored in `data/downloads` and the process and aggregated data will be in `data/outputs`.

After that, in order to calculate the factors, run the following command. --factorType argument takes in two options: average or marginal. 
```bash
python get_factor_estimates.py --factorType marginal. 
```
The results will be stored in `data/results/`. 
