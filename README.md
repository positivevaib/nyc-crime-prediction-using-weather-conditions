# nyc-crime-prediction-using-weather-conditions
Predicting crimes in New York City using weather conditions.

## Data Schemas

### Weather:

Date: String
The date, in YYYY-MM-DD format, when the weather measurement was recorded.

Time: String
The time, in HH:MM:SS format, when the weather measurement was recorded.

Temperature: Int
The dry bulb temperature recorded.

Rain: String
yes/no entry indicating rain.

Snow: String
yes/no entry indicating snow.

Fog: String
yes/no entry indicating fog.

Humidity: Int
The relative humidity recorded.

### Crime:

Date: String
The date, in DD-MM-YYYY format, when the crime was reported.

Time: String
The time, in HH:MM:SS format, when the crime was reported.

Crime Type: Int
The type of crime reported, each crime type related to an Int, 
the relationship is:
DANGEROUS WEAPONS	0
OFFENSES AGAINST MARRIAGE UNCL	1
POSSESSION OF STOLEN PROPERTY	2
BURGLARY	3
LOITERING FOR DRUG PURPOSES	4
THEFT OF SERVICES	5
OFF. AGNST PUB ORD SENSBLTY &	6
LOITERING	7
KIDNAPPING	8
FELONY ASSAULT	9
"LOITERING/GAMBLING (CARDS	10
HOMICIDE-NEGLIGENT-VEHICLE	11
NEW YORK CITY HEALTH CODE	12
FELONY SEX CRIMES	13
FORGERY	14
OTHER STATE LAWS (NON PENAL LAW)	15
ANTICIPATORY OFFENSES	16
FRAUDS	17
OTHER STATE LAWS	18
DISORDERLY CONDUCT	19
VEHICLE AND TRAFFIC LAWS	20
OFFENSES AGAINST PUBLIC SAFETY	21
ESCAPE 3	22
ADMINISTRATIVE CODES	23
NYS LAWS-UNCLASSIFIED VIOLATION	24
THEFT-FRAUD	25
ALCOHOLIC BEVERAGE CONTROL LAW	26
DISRUPTION OF A RELIGIOUS SERV	27
ABORTION	28
OFFENSES RELATED TO CHILDREN	29
INTOXICATED & IMPAIRED DRIVING	30
CRIMINAL TRESPASS	31
OFFENSES AGAINST THE PERSON	32
CRIMINAL MISCHIEF & RELATED OF	33
SEX CRIMES	34
OTHER STATE LAWS (NON PENAL LA	35
JOSTLING	36
BURGLAR'S TOOLS	37
LOITERING/DEVIATE SEX	38
OTHER OFFENSES RELATED TO THEF	39
ADMINISTRATIVE CODE	40
ENDAN WELFARE INCOMP	41
ARSON	42
GRAND LARCENY OF MOTOR VEHICLE	43
FRAUDULENT ACCOSTING	44
RAPE	45
PETIT LARCENY OF MOTOR VEHICLE	46
GAMBLING	47
OFFENSES INVOLVING FRAUD	48
ASSAULT 3 & RELATED OFFENSES	49
PROSTITUTION & RELATED OFFENSES	50
UNAUTHORIZED USE OF A VEHICLE	51
UNLAWFUL POSS. WEAP. ON SCHOOL	52
UNDER THE INFLUENCE OF DRUGS	53
ROBBERY	54
INTOXICATED/IMPAIRED DRIVING	55
KIDNAPPING & RELATED OFFENSES	56
"HOMICIDE-NEGLIGENT	57
FORTUNE TELLING	58
PETIT LARCENY	59
MISCELLANEOUS PENAL LAW	60
GRAND LARCENY	61
HARRASSMENT 2	62
MURDER & NON-NEGL. MANSLAUGHTER	63
OTHER TRAFFIC INFRACTION	64
OFFENSES AGAINST PUBLIC ADMINI	65
AGRICULTURE & MRKTS LAW-UNCLASSIFIED	66
CHILD ABANDONMENT/NON SUPPORT	67
NYS LAWS-UNCLASSIFIED FELONY	68
DANGEROUS DRUGS	69
KIDNAPPING AND RELATED OFFENSES	70
