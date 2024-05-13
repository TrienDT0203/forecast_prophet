:: This command use to install require library to run model
pip install -r setup/requirement.txt

:: init 2-folders which are _data_forecast and _data_raw
mkdir data/database
mkdir data/data_forecast

:: init database
cd src\lib\etl
python create_table.py