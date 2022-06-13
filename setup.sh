
wget https://datahub.io/core/airport-codes/r/airport-codes.csv -O dags/data/airports.csv

wget https://datahub.io/core/country-list/r/data.json -O dags/data/countries.json

kaggle datasets download arjunaraoc/india-states -f india-states.csv -p dags/data/

kaggle datasets download andradaolteanu/country-mapping-iso-continent-region -f continents2.csv -p dags/data/

mv dags/data/continents2.csv dags/data/continents.csv

python scripts/fetch_covid.py

aws s3api create-bucket --acl public-read-write --bucket {your-bucket-name} --create-bucket-configuration LocationConstraint=ap-south-1

pip install bs4
pip install requests

python scripts/fetch_opensky.py

for i in $(jq -r ". | .[]" ~/Documents/test/datasetURLs.json)
  do
    wget $i
    gunzip $(basename $i)
    aws s3 cp $(basename $i .gz) s3://{your-bucket-name}/raw/opensky/$(basename $i .gz)
    rm $(basename $i .gz)
  done


