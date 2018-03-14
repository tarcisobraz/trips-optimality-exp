# Public Transportation Trips Evaluation
This repo holds code and documentation of a research experiment whose goal is to evaluate the inefficiency of user trip choices in Public Transportation.

This experiment is part of the EUBra-BIGSEA project (690116), a Research & Innovation Action (RIA) funded by the European Commission under the Co-operation Programme, Horizon 2020 and the Ministério de
Ciência, Tecnologia e Inovacao (MCTI), RNP/Brazil.

## The Data

The data used for this experiment is comprised of three data sources:

- Transit Routes and Schedule (GTFS)
- Vehicle Location (AVL)
- Fare Collection (AFC)

<Data Sample>

## The Experiment

The Experiment consists of a series of processing steps:

1 - Data Integration

  * BULMA - Match Vehicles GPS trajectory (AVL) to predefined shape trajectory (GTFS)
  * BUSTE - Match passenger boarding (AFC) to vehicle trajectory (BULMA result)

2 - Trips Inference

  * Origin-Destination Matrix Estimation
  
3 - Trips Evaluation
 
  * Inefficiency Measurement

## The Results

Awaiting submitted paper publication decision.
