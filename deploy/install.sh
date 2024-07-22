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
    git checkout rpc
else
    cd trabalho-sistemas-distribuidos
    git checkout rpc
    git pull
fi

pip install rpyc

echo Killing previous process
pkill -f datanode.py

echo Starting datanode.py
### ALTERAR localhost para o ip da máquina que está rodando o registry
python3 datanode.py localhost 18811 &

disown
