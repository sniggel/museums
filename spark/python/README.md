# Museums

## How to run
Simply invoke docker-compose up, all the images are publicly accessible on docker hub.

### How it works
It runs a pyspark application inside a docker image.

The spark code performs a few things:

- fetch the list of most visited museums in the world

- fetch the list of the most populated cities in the world

- fetch extra museums characteristics (like foundation date, nb. of pieces, size of the museum, etc...)

- fetch other population (those who weren't in the above page)

All data is parsed using BeautifulSoup4, cleaned at the source, converted to spark DataFrames, joined and finally pushed into a postgresql database.

The Database schema is as follow (columns):

- name: The name of the museum.

- wikilink: The wikipedia link of the museum.

- city: The city where the museum is located.

- visitors: The number of visitors per year of the museum.

- characteristics: The extra characteristics, this is a json column.

- city_population: The population of the city in which the museum is located.

### note to self
To build:
1. run build-dependencies.sh
2. verify configs folder
3. verify docker-compose.yml docker tags
4. to deploy on docker hub, login first then invoke docker-build.sh

To run:
To run locally, always use run_etl.sh
