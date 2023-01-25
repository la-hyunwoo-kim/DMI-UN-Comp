# DMI UN Comparison

## Introduction
This repo contains the source code for UN Comparison.
This report covers the UNCON, UNTAL, and UNOT provenances.

## Installation
The repo uses [poetry](https://python-poetry.org/docs/) as package manager.
After cloning the repo locally run ```poetry install``` to install the required packages.

## Running
To run the the script use ```python app.py``` on the root directory.
There are a number of arguments that can be used with the script. 

1. ```-ss``` Saves the source data and DMI data locally under ```./out/test-out/```
2. ```-sdl``` Skips downloading of data from source and DMI database and instead uses downloaded source under ```./out/test-out/```. Note that if there are no previously downloaded source, this command will fail.

A ```client_token.json``` file in the root folder is required for the following 3 arguments:
1. ```-gg``` Uses the Drive folders defined under ```config.json``` as output folder.
2. ```-em``` Sends report email containing the output report
3. ```-t``` Activates testing flag. Adding ```-t``` in the argument changes the recipient of the email to yourself adn the output Drive folder to the testing folder.
