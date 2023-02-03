@ECHO OFF 

pip install -r requirements.txt

tar -xf Data.zip

ECHO "Data.zip unzipped successfully"

spark-submit --master local[*] main_module.py "configuration/config.json"

