#!/bin/bash

sudo apt-get update -y
sudo apt-get install git -y

sudo apt-get install apache2 -y
sudo apt-get install libapache2-mod-wsgi -y

wget https://bootstrap.pypa.io/ez_setup.py -O - | sudo python
sudo easy_install pip
sudo pip install virtualenv
sudo pip install virtualenvwrapper

echo "export WORKON_HOME=$HOME/.virtualenvs" >> ~/.bash_profile
echo "export PROJECT_HOME$HOME/insight/server" >> ~/.bash_profile
echo "source /usr/local/bin/virtualenvwrapper.sh" >> ~/.bash_profile
source ~/.bash_profile

git clone https://github.com/isuraed/insight.git
mkvirtualenv flask
pip install flask
pip install happybase

sudo cp insight/server/default /etc/apache2/sites-available/
sudo /etc/init.d/apache2 restart

screen -S server
python insight/server/server.py
