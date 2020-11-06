# Astrolobot
A project that builds a database of horoscopes and teaches a CNN the how-tos of a horoscope

## Installation
This project uses Python 3.6.* and TensorFlow
* install TensorFlow `pip3 install --upgrade TensorFlow`
* build a database with collector `collector.py -s YYYY-MM-DD -e YYYY-MM-DD` (this should take some time)

## Linting
Astrolobot uses Black for linting, if it's not already installed, run `pip install black` to install it.

Before merging with master, make sure your code is correctly formatted according to black, you can automate this by running `black .` in the directory you added code to.