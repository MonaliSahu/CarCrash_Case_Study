@ECHO OFF 

pip install pyspark
pip install pathlib

tar -xf Data.zip

ECHO "Data.zip unzipped successfully"

cd Source_code

ECHO "Inside Source_code"

spark-submit --master local[*] main_module.py "config.json"
