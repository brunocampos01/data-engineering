### About **DESAFIO ESTÁGIO DATA ENGINEERING**
A sua tarefa é escrever uma pequena ETL usando esses arquivos de input e gerando dois outputs.

Quickstart
----------
Abrir o arquivo (.ipynb)[https://github.com/brunocampos01/challenges/blob/master/indicium/DESAFIO%20EST%C3%81GIO%20DATA%20ENGINEERING.ipynb] 
**Example Output**

![Worm Crawling](img/worm-crawling.gif)

<img src="img/muscle-activity.png" width="250"><img src="img/neuron-activity.png" width="350">

**NOTE**: Running the simulation for the full amount of time would produce content like the above.  However, in order to run in a reasonable amount of time, the default run time for the simulation is limited.  As such, you will see only a partial output, equivalent to about 5% of run time, compared to the examples above.  To extend the run time, use the `-d` argument as described below.

**Installation**

Pre-requisites:

1. Currently Windows is not supported (see https://github.com/openworm/OpenWorm/issues/263); you will need Mac OS or Linux (or a virtual environment on Windows that runs either of those).
2. You should have at least 60 GB of free space on your machine and at least 2GB of RAM

To Install:

1. Install [Docker](http://docker.com) on your system.  
2. If your system does not have enough free space, you can use
an external hard disk.  On MacOS X, the location for image storage
can be specified in the [Advanced Tab](https://forums.docker.com/t/change-docker-image-directory-for-mac/18891/15) in Preferences.  See [this thread](https://forums.docker.com/t/how-do-i-change-the-docker-image-installation-directory/1169/18)
in addition for Linux instructions.  

**Running**

1. Open a terminal and run: `git clone http://github.com/openworm/openworm`; `cd openworm`
1. Run `./run.sh`.
2. About 5-10 minutes of output will display on the screen as the steps run.
3. The simulation will end.  Exit the container with `exit` and run `stop.sh` on your system to clean up the running container.
4. Inspect the output in the `output` directory.
