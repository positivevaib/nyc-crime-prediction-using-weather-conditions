from flask import render_template
from app import app
from flask import redirect, url_for, request
import os

All_path = "app/predictions/overall"
Burglary_path = "app/predictions/burglary"
Assualt_path = "app/predictions/assault"
Rape_path = "app/predictions/rape"

All = os.listdir(All_path)
Burglary = os.listdir(Burglary_path)
Assualt = os.listdir(Assualt_path)
Rape = os.listdir(Rape_path)


@app.route('/input',methods = ['POST', 'GET'])
def input():
   if request.method == 'POST':
      temp = request.form['temperature']
      humd = request.form['humidity']
      if request.form.get('rain') == 'on':
         rain = '1'
      else:
         rain = '0'
      if request.form.get('snow') == 'on':
         snow = '1'
      else:
         snow = '0'
      if request.form.get('fog') == 'on':
         fog = '1'
      else:
         fog = '0'

      WeatherString = str(int(round(float(temp)/10, 0)*10)) + ',' + rain + ',' + snow + ',' + fog + ',' + str(int(round(float(humd)/10, 0)*10))
      

      # Calculate Percentile for All Crime Type
      tmp = ''
      tmpList = []
      Ci = 0
      Fi = 0
      N = 880
      for i in All:
         if i[0:4] == 'part':
            f = open(All_path + '/'+ i)
            for line in f:
               LineList = line.split(',')
               tmpList.append(int(float(LineList[5].strip('\n'))))
               LineString = LineList[0] + ',' + LineList[1] + ',' + LineList[2] + ',' + LineList[3] + ',' + LineList[4]
               if WeatherString == LineString:
                  tmp = int(float(LineList[5].strip('\n')))
      for i in tmpList:
         if i < tmp:
            Ci = Ci + 1
         if i == tmp:
            Fi = Fi + 1
      All_Percentile = round((Ci + 0.5 * Fi) / N * 100, 2)
         
      # Calculate Percentile for Burglary crime type
      tmp = ''
      tmpList = []
      Ci = 0
      Fi = 0
      N = 880
      for i in Burglary:
         if i[0:4] == 'part':
            f = open(Burglary_path + '/'+ i)
            for line in f:
               LineList = line.split(',')
               tmpList.append(int(float(LineList[5].strip('\n'))))
               LineString = LineList[0] + ',' + LineList[1] + ',' + LineList[2] + ',' + LineList[3] + ',' + LineList[4]
               if WeatherString == LineString:
                  tmp = int(float(LineList[5].strip('\n')))
      for i in tmpList:
         if i < tmp:
            Ci = Ci + 1
         if i == tmp:
            Fi = Fi + 1
      Burglary_Percentile = round((Ci + 0.5 * Fi) / N * 100, 2)

      # Calculate Percentile for Assualt crime type
      tmp = ''
      tmpList = []
      Ci = 0
      Fi = 0
      N = 880
      for i in Assualt:
         if i[0:4] == 'part':
            f = open(Assualt_path + '/'+ i)
            for line in f:
               LineList = line.split(',')
               tmpList.append(int(float(LineList[5].strip('\n'))))
               LineString = LineList[0] + ',' + LineList[1] + ',' + LineList[2] + ',' + LineList[3] + ',' + LineList[4]
               if WeatherString == LineString:
                  tmp = int(float(LineList[5].strip('\n')))
      for i in tmpList:
         if i < tmp:
            Ci = Ci + 1
         if i == tmp:
            Fi = Fi + 1
      Assualt_Percentile = round((Ci + 0.5 * Fi) / N * 100, 2)

      # Calculate Percentile for Rape crime type
      tmp = ''
      tmpList = []
      Ci = 0
      Fi = 0
      N = 880
      for i in Rape:
         if i[0:4] == 'part':
            f = open(Rape_path + '/'+ i)
            for line in f:
               LineList = line.split(',')
               tmpList.append(int(float(LineList[5].strip('\n'))))
               LineString = LineList[0] + ',' + LineList[1] + ',' + LineList[2] + ',' + LineList[3] + ',' + LineList[4]
               if WeatherString == LineString:
                  tmp = int(float(LineList[5].strip('\n')))
      for i in tmpList:
         if i < tmp:
            Ci = Ci + 1
         if i == tmp:
            Fi = Fi + 1
      Rape_Percentile = round((Ci + 0.5 * Fi) / N * 100, 2)
      return redirect(url_for('output',Prank1 = All_Percentile, Prank2 = Burglary_Percentile, Prank3 = Assualt_Percentile, Prank4 = Rape_Percentile))
   return render_template('input.html')
   #else:
      #user = request.args.get('nm')
      #return redirect(url_for('success',name = user))
@app.route('/output/<Prank1>/<Prank2>/<Prank3>/<Prank4>')
def output(Prank1, Prank2, Prank3, Prank4):
   return render_template('output.html', Prank1 = Prank1, Prank2 = Prank2, Prank3 = Prank3, Prank4 = Prank4)
