## Public Transportation Trips Evaluation

### Table of Contents

1. [Installation](#installation)
2. [Motivation](#motivation)
3. [Repository Structure / Files](#files)
4. [Experiment Description](#experiment)
4. [Results](#results)
5. [Licensing, Authors, and Acknowledgements](#licensing)

## Installation <a name="installation"></a>

The code assumes you use Anaconda (Python 3) with the following extra libraries installed: geoplot, geopandas, shapely (use the following installation command): `conda install -c conda-forge geoplot geopandas shapely`

## Motivation<a name="motivation"></a>

This repo holds code and documentation of a research experiment which is part of my Masters research in Public Transportation Data Analysis. The goal of this experiment is to evaluate the inefficiency of user trip choices in Public Transportation.

## Repository Structure / Files <a name="files"></a>

The `notebooks` folder contains the draft notebooks for each of the experiment analysis steps. The `scripts` folder contains the final scripts for each of the experiment analysis steps, which were used to generate the results and visualizations for the final version of the publication.

## Experiment Description<a name="experiment"></a>

### The Data

The data used for this experiment is comprised of three data sources:

  * Transit Routes and Schedule (GTFS)
  * Vehicle Location (AVL)
  * Fare Collection (AFC)

### The Experiment

The Experiment consists of a series of processing steps:

1 - Data Integration

  * BULMA - Match Vehicles GPS trajectory (AVL) to predefined shape trajectory (GTFS)
  * BUSTE - Match passenger boarding (AFC) to vehicle trajectory (BULMA result)

2 - Trips Inference

  * Origin-Destination Matrix Estimation
  
3 - Trips Evaluation
 
  * Inefficiency Measurement

## Results<a name="results"></a>

The results of this experiment, as well as a more detailed description of each step have been published in IEEE Transactions on Intelligent Transportation Systems journal, in a publication entitled: [Estimating Inefficiency in Bus Trip Choices From a User Perspective With Schedule, Positioning, and Ticketing Data](https://ieeexplore.ieee.org/document/8405755)

## Licensing, Authors, Acknowledgements<a name="licensing"></a>

This experiment is part of the EUBra-BIGSEA project (690116), a Research & Innovation Action (RIA) funded by the European Commission under the Co-operation Programme, Horizon 2020 and the Ministério de
Ciência, Tecnologia e Inovacao (MCTI), RNP/Brazil.

Feel free to use the code provided that you give credits / cite this repo and the publication, as well as to contribute.
