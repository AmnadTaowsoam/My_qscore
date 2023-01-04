# My_qscore
Quality score


## directory

    mkdir documents
    cd documents
    mkdir qscore
    mkdir rawdata
    mkdir vendor_material
    cd rawdata
    mkdir feedmill_rawdata
    mkdir notification_rawdata
    mkdir qa32_rawdata

## Environment
command line:

    python -m venv env
    env\Scripts\activate.bat

library requirements:
command line:

    pip install -r requirements.txt

## create Database
command line:

        python setup.py

## data to calculate
    1. copyfile ZTQMR and past to Folder: rawdata/notification_rawdata
    2. copyfile ZTMR and past to Folder: rawdata/feedmil_rawdata
    3. copyfile QA32 and past to Folder: rawdata/qa32_rawdata

## calculate
    1. q_score = quality score - NC Score(99 point form 1 NC:type ZC)
    2. Averaged (q_score) set_index Vendor, material
    3. select best recorde (window=4) and select last record
