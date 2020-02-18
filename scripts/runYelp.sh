#!/bin/bash

#bash runExperiments.sh 5 Data/yelpFull.json logOutputs/yelp.Baazizi.log 300000 7437120 1.0
bash runExperiments.sh 5 Data/yelp/business.json logOutputs/yelpBusiness.Baazizi.log 10000 156639 1.0
bash runExperiments.sh 5 Data/yelp/checkin.json logOutputs/yelpCheckin.Baazizi.log 10000 135148 1.0
bash runExperiments.sh 5 Data/yelp/photos.json logOutputs/yelpPhotos.Baazizi.log 10000 196278 1.0
bash runExperiments.sh 5 Data/yelp/review.json logOutputs/yelpReview.Baazizi.log 300000 4736897 1.0
bash runExperiments.sh 5 Data/yelp/tip.json logOutputs/yelpTip.Baazizi.log 30000 1028802 1.0
bash runExperiments.sh 5 Data/yelp/user.json logOutputs/yelpUser.Baazizi.log 30000 1183362 1.0