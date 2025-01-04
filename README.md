# Data Project Portai¿folio

<a target="_blank" href="https://www.linkedin.com/in/marti-hm/">
    <img src="https://img.shields.io/badge/LinkedIn-Profile-0077b5" alt="LinkedIn Profile" />
</a>

## Objective
This repository aims to showcase various code samples that represent my past work, thought processes, folder organization, and coding skills. For security reasons, I have not uploaded the `.env` file. However, feel free to contact me at martin.hidalgom1@gmail.com if you'd like to test and run any of these codes (if necessary).

## Repository Organization

```
├── LICENSE            <- Open-source license if one is chosen
├── README.md          <- The top-level README for programmers using this project
├── config             <- Configuration files for the project, including global variables, API keys, and logging settings
├── data
│   ├── external       <- Data from third party sources
│   ├── interim        <- Intermediate data that has been transformed
│   ├── processed      <- The final, canonical data sets for modeling
│   └── raw            <- The original, immutable data dump
│
├── models             <- Trained and serialized models, model predictions, or model summaries
│
├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
│                         the creator's initials, and a short `-` delimited description, e.g.
│                         `1.0-jqp-initial-data-exploration`
│
├── references         <- Data dictionaries, manuals, and all other explanatory materials
│
├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
│   └── figures        <- Generated graphics and figures to be used in reporting
│
├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
│                         generated with `pip freeze > requirements.txt`
│
└── src                         <- Source code for this project
    │
    ├── __init__.py             <- Makes src a Python module
    │
    ├── config.py               <- Store useful variables and configuration
    │
    ├── dataset.py              <- Scripts to download or generate data
    │
    ├── features.py             <- Code to create features for modeling
    │
    │    
    ├── modeling                
    │   ├── __init__.py 
    │   ├── predict.py          <- Code to run model inference with trained models          
    │   └── train.py            <- Code to train models
    │
    ├── plots.py                <- Code to create visualizations 
    │
    └── services                <- Service classes to connect with external platforms, tools, or APIs
        └── __init__.py 
```

--------