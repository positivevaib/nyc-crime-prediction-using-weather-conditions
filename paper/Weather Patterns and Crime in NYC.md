# Weather Patterns and Crime in NYC

## ABSTRACT
The project aims to analyze the correlations between weather patterns and the crime rates in NYC. We want to understand how weather affects different types of crimes and the application is aimed at the NYPD and other similar police departments to help them manage crime better, and normal people can use our application to see if the place he will go through at that time is safe enough and if he should go outside now.

## 1. INTRODUCTION
People's moods always change with the weather. For example, in the rainy winter, people are more likely to be depressed and unhappy, and in the sunny spring, people will be more vibrant. Emotions often dominate people's behavior. When a person shows negative emotions, he will be more likely to make impulses, hurt others, and even commit crimes. Therefore, we believe that there is relationship between weather and crime rate. 

Based on this relationship, we can help the police department solve some practical problems. There are 686665 law enforcement officer in United States in 2018[1], it is a huge number. When you drive, or walk outside, you can often see police officers in many places, and their presence does not indicate that this area is very unsafe now, but the police are assigned to this area, and Shifts need to be constantly guarding this area. No matter what day it is, the number of police officers in each area will not change much every day.  

In many cases, the police simply park the police car there, and then should wait constantly to see if there is a crime. Maybe today is a day when the crime rate is high, the police will be in short supply, and sometimes, today is a day when the crime rate is low, the police will do nothing, this is a very inefficient behavior.   

When we found out the relationship between the weather and the crime rate, everything was solved. Through the continuous accumulation and analysis of historical data, we can analyze whether the current period of time in this area is the time of high crimes through the current weather conditions in this area. When it is analyzed that the crime rate is high, the police station can deploy in advance to release more police force, and when it is analyzed that the crime rate is low in this period today, the police station can arrange fewer police officers to patrol outside. Even passers-by can analyze to determine whether it is a suitable time to go out.

## 2. DATA
New York is the most prosperous city in the United States, and even the most prosperous city in the world. it even has the reputation of the city that never sleeps. Prosperity brings not only wealth, but also more crime. New York City has a lot of crimes, giving us enough crime data to analyze. Moreover, there is a base station in Manhattan for various weather analysis, which gives us enough weather data for analysis.
### 2.1. Crime Data
We collect crime data from NYC OpenData. The original dataset is about all the crime complaints reported to Police Department. And the data is from police department, so the dataset is reliable.It contains Randomly generated persistent ID for each complaint, Exact date of occurrence for the reported event and 32 more information about crime. Since we need to find out the relationship between weather, we only need the date, time, crime type and number of crime happened at that time.
#### Data Schema:
#### Cleaned Data:

### 2.2. Weather Data
NOAA is the national centers for environmental information, it contains all kind of different dataset in the world. For this project, we only need to use New York City's weather data. So we find the data in Local Climatological Data(LCD) part. 
#### Data Schema:
#### Cleaned Data:
## 3. ANALYSIS

## 4.CONCLUSION 
## 5. reference
[1]https://www.statista.com/statistics/191694/number-of-law-enforcement-officers-in-the-us/