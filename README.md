# AndesMollusque
[![Docs](https://github.com/iml-gddaiss/AndesMollusque/actions/workflows/documentation.yml/badge.svg?branch=main)](https://github.com/iml-gddaiss/AndesMollusque/actions/workflows/documentation.yml)


Map entre la BD Andes et les tables mollusque de PSENTINELLE_PRO

The documentation available at: [https://iml-gddaiss.github.io/AndesMollusque/](https://iml-gddaiss.github.io/AndesMollusque/)

# Quickstart
After creating (and activating) a virtual python environment, install requirements:
```
python -m pip install - requirements.txt
```

copy the `.env_sample` to `.env` and fill in required values.

# DFO Windows oracleDb instant client
From centre logiciel, install `Oracle 12 (Instant Client) x64` which should create the client libraries under "C:\Oracle\12.2.0_Instant_x64".
This path is needed by the python client to use thick-mode `oracledb.init_oracle_client(lib_dir=r"C:\Oracle\12.2.0_Instant_x64")`

