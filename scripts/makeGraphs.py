import matplotlib.pyplot as plt
import json
import math

datasetName = "synapse"

with open(datasetName.lower()+'.log') as f:
    lines = f.readlines()
    x = [int(json.loads(line)['TestSize']) for line in lines]
    flatPrecision = [math.log(int(json.loads(line)['FlatPrecision'])) for line in lines]
    verbosePrecision = [math.log(int(json.loads(line)['VerbosePrecision'])) for line in lines]
    ourPrecision = [math.log(int(json.loads(line)['Precision'])) for line in lines]

    flatValidation = [float(json.loads(line)['FlatValidation']) for line in lines]
    verboseValidation = [float(json.loads(line)['VerboseValidation']) for line in lines]
    ourValidation = [float(json.loads(line)['Validation']) for line in lines]

    fig = plt.figure(figsize=(20,7))
    plt.subplot(121)
    plt.plot(x, flatPrecision, 'go-', alpha = .5)
    plt.plot(x, verbosePrecision, 'o--', alpha = .5)
    plt.plot(x, ourPrecision, 'r+-', alpha = .5)
    plt.xlabel('Number of Training Rows')
    plt.ylabel('Log Precision')
    plt.title(datasetName+"'s Log Precision")
    plt.legend(['Flat','Verbose','Ours'])
    #fig.savefig(datasetName.lower()+"precision.png")

    plt.subplot(122)
    plt.plot(x, flatValidation, 'go-', alpha = .5)
    plt.plot(x, verboseValidation, 'o--', alpha = .5)
    plt.plot(x, ourValidation, 'r+-', alpha = .5)
    plt.xlabel('Number of Training Rows')
    plt.ylabel('Validation %')
    plt.title(datasetName+"'s Validation")
    plt.legend(['Flat','Verbose','Ours'])
    #plt.show()
    plt.savefig(datasetName.lower()+"T.png")
