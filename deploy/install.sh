#!/bin/bash
cd ~
if ! [ -d sistemas_distribuidos ]
then
    mkdir sistemas_distribuidos
fi
cd sistemas_distribuidos

if ! [ -d trabalho-sistemas-distribuidos ]
then
    git clone https://github.com/gabrielcrispimvs/trabalho-sistemas-distribuidos.git
    cd trabalho-sistemas-distribuidos
    git checkout rabbitmq
else
    cd trabalho-sistemas-distribuidos
    git checkout rabbitmq
    git pull
fi

pip install rpyc
pip install pika

echo Killing previous process
pkill -SIGINT -f datanode.py
pkill -SIGINT -f datanode.py

echo Starting datanode.py
### ALTERAR 192.168.40.39 para o ip da máquina que está rodando o registry
cd datanode
python3 datanode.py 192.168.40.39 5672 > log.txt &

disown
