# Museums
A new world organization has just been created. It includes all the museum management committees that have more than 2,000,000 visitors annually (in 2017). This list is available via Wikipedia: https://en.wikipedia.org/wiki/List_of_most_visited_museums

This new organization wishes to correlate the tourist attendance at their museums with the population of the respective cities. To achieve this, a small, common and harmonized database must be built to be able to extract features. This DB must include the characteristics of museums as well as the population of the cities in which they are located. You have been chosen to build this database. In addition, you are asked to create a small linear regression ML algorithm to correlate the city population and the influx of visitors. You must use the Wikipedia APIs to retrieve this list of museums and their characteristics. You are free to choose the source of your choice for the population of the cities concerned.

It is required that your code is in Python and you have done some R&D work in a Jupyter notebook in python (which can be executed locally or via a web-hosted platform such as Colab: https://colab.research.google.com/notebooks/welcome.ipynb) using some visualization.

It is also required that your code can be executed in a Docker container (use Docker Compose if you require additional infrastructure).

You will be evaluated not only on how your code works but also on the rationale for the choices you make.

# Project structure
There are 2 sub-projects within this project:

1. jupyter-notebook: A jupyter notebook developed with anaconda. It mainly uses pandas, numpy, matplotlib and sklearn to fetch wikipedia pages, parse them and apply linear regression to the data.
2. spark/python: A production approach to generated a postgres database. It combines the usage of pyspark and beautifulsoup4 to parse wikipedia pages into a working database.

# Folder structure
```
./
├── jupyter-notebook (proof of concept for the above description, see [README.md](jupyter-notebook/README.md))
│   ├── environment.yml
│   ├── museums.ipynb
│   └── README.md
└── spark (a production approach to creating the database, see [README.md](spark/python/README.md))
    ├── python
    ...
```

## Some notes
### Run a postgres docker
docker run --name museums -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=museums -d postgres

### Connect to running instance using pgcli
pgcli postgres://postgres:postgres@localhost:5432/museums
