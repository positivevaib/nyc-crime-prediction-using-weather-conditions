# nyc-crime-prediction-using-weather-conditions
Predicting overall crime rate and the expected rates of assault, burglary and rape in New York City using weather conditions.

The relationships between weather conditions and crimes were modelled using linear regression analysis with quadratic terms.  

The independent variables included:  
- Temperature reading in deg. Fahrenheit  
- Squared temperature reading in deg. Fahrenheit  
- Thre dummy variables indicating the presence of rain, snow or fog  
- Relative humidity  
- Squared relative humidity  

The dependent variable was the number of complaints received by the NYPD regarding the crime of interest over the 12 year period from January 1, 2006 to December 31, 2017.  

Thus, the models took the following form.  
C = &theta;<sub>o</sub>temp + &theta;<sub>1</sub>temp<sup>2</sup> + &theta;<sub>2</sub>rain + &theta;<sub>3</sub>snow + &theta;<sub>4</sub>fog + &theta;<sub>5</sub>humidity + &theta;<sub>6</sub>humidity<sup>2</sup>  

The models' R2 values are listed below:  
- Overall: 0.3037  
- Assault: 0.3301  
- Burglary: 0.2695  
- Rape: 0.2137  
