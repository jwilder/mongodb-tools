### mongodb-tools - Utilities for working MongoDB

* collection-stats.py - Display statistics about the collections in all databases
* index-stats.py - Displays statistics about the indexes in all databases
* redudant-indexes.py - Finds indexes that may redundant

## Dependencies

    python-virtualenv

## Installation

    git clone https://github.com/jwilder/mongodb-tools
    cd mongodb-tools
    ./setup.sh
    source virtualenv/bin/activate

## Test Data Setup

    In on terminal run:

    $ ./run-mongo.sh


    In another terminal run:

    $ python examples/testdata.py


## collection-stats.py ##

     $ ./collection-stats.py

     Checking DB: examples2.system.indexes
     Checking DB: examples2.things
     Checking DB: examples1.system.indexes
     Checking DB: examples1.address
     Checking DB: examples1.typeless_address
     Checking DB: examples1.user
     Checking DB: examples1.typeless_user


     +----------------------------+--------+--------+---------+--------------+---------+------------+
     |         Collection         | Count  | % Size | DB Size | Avg Obj Size | Indexes | Index Size |
     +----------------------------+--------+--------+---------+--------------+---------+------------+
     | examples1.address          |      2 |   0.0% | 184.00b |       92.00b |    2    |     15.97K |
     | examples1.system.indexes   |      9 |   0.0% | 912.00b |      101.33b |    0    |      0.00b |
     | examples1.typeless_address |      2 |   0.0% | 216.00b |      108.00b |    1    |      7.98K |
     | examples2.system.indexes   |      2 |   0.0% | 164.00b |       82.00b |    0    |      0.00b |
     | examples1.typeless_user    | 101879 |  26.7% |  10.10M |      104.00b |    3    |      8.18M |
     | examples1.user             | 101879 |  36.0% |  13.60M |      140.00b |    3    |     15.20M |
     | examples2.things           | 100000 |  37.3% |  14.11M |      148.00b |    2    |      5.67M |
     +----------------------------+--------+--------+---------+--------------+---------+------------+
     Total Documents: 303773
     Total Data Size: 37.82M
     Total Index Size: 29.08M
     RAM Headroom: 2.87G
     RAM Used: 2.74G (61.6%)
     Available RAM Headroom: 1.10G

## index-stats.py

    $ ./index-stats.py
    

    Checking DB: examples2.system.indexes
    Checking DB: examples2.things
    Checking DB: examples1.system.indexes
    Checking DB: examples1.address
    Checking DB: examples1.typeless_address
    Checking DB: examples1.user
    Checking DB: examples1.typeless_user

    Index Overview
    +----------------------------+------------------------+--------+------------+
    |         Collection         |         Index          | % Size | Index Size |
    +----------------------------+------------------------+--------+------------+
    | examples1.address          | _id_                   |   0.0% |      7.98K |
    | examples1.address          | _types_1               |   0.0% |      7.98K |
    | examples1.typeless_address | _id_                   |   0.0% |      7.98K |
    | examples1.typeless_user    | _id_                   |  10.9% |      3.17M |
    | examples1.typeless_user    | address_id_1           |  10.9% |      3.17M |
    | examples1.typeless_user    | typeless_address_ref_1 |   6.4% |      1.85M |
    | examples1.user             | _id_                   |  10.9% |      3.17M |
    | examples1.user             | _types_1_address_id_1  |  13.2% |      3.84M |
    | examples1.user             | _types_1_address_ref_1 |  28.2% |      8.20M |
    | examples2.things           | _id_                   |  10.7% |      3.11M |
    | examples2.things           | _types_1               |   8.8% |      2.56M |
    +----------------------------+------------------------+--------+------------+

    Top 5 Largest Indexes
    +-------------------------+------------------------+--------+------------+
    |        Collection       |         Index          | % Size | Index Size |
    +-------------------------+------------------------+--------+------------+
    | examples1.user          | _types_1_address_ref_1 |  28.2% |      8.20M |
    | examples1.user          | _types_1_address_id_1  |  13.2% |      3.84M |
    | examples1.typeless_user | _id_                   |  10.9% |      3.17M |
    | examples2.things        | _id_                   |  10.7% |      3.11M |
    | examples2.things        | _types_1               |   8.8% |      2.56M |
    +-------------------------+------------------------+--------+------------+

    Total Documents: 303773
    Total Data Size: 37.82M
    Total Index Size: 29.08M
    RAM Headroom: 2.87G
    RAM Used: 2.73G (61.4%)
    Available RAM Headroom: 1.11G
