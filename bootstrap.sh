#! /bin/sh

pip install -r requirements.txt

unzip Data.zip

echo "Data.zip unzipped successfully"

spark-submit --master local[*] main_module.py "configuration/config.json"

