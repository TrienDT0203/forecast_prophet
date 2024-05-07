:: This command use to install require library to run model
pip install -r requirement.txt

:: init 2-folders which are _data_forecast and _data_raw
mkdir _data_forecast
mkdir _data_raw

:: init database
cd _script/create_database
python create_table.py