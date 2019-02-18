import matplotlib.pyplot as plt
import numpy as np
import json
import math

datasetName = "Github"

with open(datasetName.lower()+'.log') as f:
    lines = f.readlines()
    x = [int(json.loads(line)['TestSize']) for line in lines]
    flatPrecision = [math.log(int(json.loads(line)['FlatPrecision'])) for line in lines]
    verbosePrecision = [math.log(int(json.loads(line)['VerbosePrecision'])) for line in lines]
    ourPrecision = [math.log(int(json.loads(line)['Precision'])) for line in lines]

    flatValidation = [float(json.loads(line)['FlatValidation']) for line in lines]
    verboseValidation = [float(json.loads(line)['VerboseValidation']) for line in lines]
    ourValidation = [float(json.loads(line)['Validation']) for line in lines]


    fig, ax = plt.subplots()
    ax.plot(x, flatPrecision, color='g', alpha = .5)
    ax.plot(x, verbosePrecision, color='orange', alpha = .5)
    ax.plot(x, ourPrecision, color='red', alpha = .5)
    ax.set_xlabel('Number of Training Rows')
    ax.set_ylabel('Log Precision')
    ax.set_title(datasetName+"'s Log Precision")
    ax.legend(['Flat','Verbose','Ours'])
    fig.savefig(datasetName.lower()+"precision.png")

    fig, ax = plt.subplots()
    ax.plot(x, flatValidation, color='g', alpha = .5)
    ax.plot(x, verboseValidation, color='orange', alpha = .5)
    ax.plot(x, ourValidation, color='red', alpha = .5)
    ax.set_xlabel('Number of Training Rows')
    ax.set_ylabel('Validation %')
    ax.set_title(datasetName+"'s Validation")
    ax.legend(['Flat','Verbose','Ours'])
    fig.savefig(datasetName.lower()+"validation.png")