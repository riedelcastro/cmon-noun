#!/bin/bash
rsync --exclude target --exclude .git --exclude models/faust-factorie --exclude debug --exclude output -avz . riedel@Vinci8.cs.umass.edu:~/vinci8_data1/projects/cmon-noun
